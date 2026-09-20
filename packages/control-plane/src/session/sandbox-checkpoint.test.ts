import { beforeEach, describe, expect, it, vi } from "vitest";
import { SandboxPreservation } from "./sandbox-preservation";
import { GENERATION, fixture, provider, readyFinite } from "./sandbox-preservation.test-fixtures";

describe("Sandbox checkpoint operations", () => {
  beforeEach(() => vi.restoreAllMocks());

  it("keeps normal work ready, serializes capture, and records captured runtime provenance", async () => {
    let complete!: (result: { success: boolean; imageId: string }) => void;
    const takeSnapshot = vi.fn(
      () =>
        new Promise<{ success: boolean; imageId: string }>((resolve) => {
          complete = resolve;
        })
    );
    const f = fixture(provider({ takeSnapshot }));
    await readyFinite(f);
    f.deps.messenger.broadcast.mockClear();
    const capture = f.preservation.checkpoint("execution_complete");
    expect(f.store.value?.checkpoint).toMatchObject({
      phase: "capturing",
      nonDestructive: true,
      providerObjectId: "provider-object-1",
    });
    expect(f.preservation.mayDispatch()).toBe(true);
    expect(f.sandboxRow.status).toBe("ready");
    expect(await f.preservation.checkpoint("duplicate")).toEqual({
      kind: "skipped",
      reason: "capture_owned",
    });
    await vi.waitFor(() => expect(takeSnapshot).toHaveBeenCalledOnce());
    f.sandboxRow.runtime_version = "runtime-2";
    complete({ success: true, imageId: "checkpoint-image" });
    expect(await capture).toMatchObject({ kind: "completed", imageId: "checkpoint-image" });
    expect(f.deps.sandbox.recordSandboxSnapshot).toHaveBeenCalledWith(
      "sandbox-1",
      "checkpoint-image",
      "runtime-1"
    );
    expect(f.preservation.mayAcquire()).toBe(true);
    expect(f.deps.messenger.broadcast.mock.calls.map(([m]) => m.type)).toEqual(["snapshot_saved"]);
    expect(f.deps.completePreservation).not.toHaveBeenCalled();
  });

  it.each(["stopped", "stale", "failed"] as const)(
    "never restores ready after a concurrent %s transition",
    async (status) => {
      const f = fixture(
        provider({
          takeSnapshot: vi.fn(async () => {
            f.sandboxRow.status = status;
            return { success: true, imageId: "checkpoint" };
          }),
        })
      );
      await readyFinite(f);
      expect(await f.preservation.checkpoint("execution_complete")).toMatchObject({
        kind: "completed",
      });
      expect(f.sandboxRow.status).toBe(status);
      expect(f.deps.completePreservation).not.toHaveBeenCalled();
    }
  );

  it.each(["generation", "provider_object", "operation"])(
    "rejects late completion after %s replacement",
    async (field) => {
      const f = fixture(
        provider({
          takeSnapshot: vi.fn(async () => {
            if (field === "generation") f.sandboxRow.created_at++;
            if (field === "provider_object") f.sandboxRow.modal_object_id = "replacement";
            if (field === "operation")
              f.store.write({
                ...f.store.value!,
                checkpoint: { ...f.store.value!.checkpoint!, operationId: "replacement" },
              });
            return { success: true, imageId: "old-image" };
          }),
        })
      );
      await readyFinite(f);
      expect(await f.preservation.checkpoint("execution_complete")).toEqual({
        kind: "skipped",
        reason: "superseded",
      });
      expect(f.deps.sandbox.recordSandboxSnapshot).not.toHaveBeenCalled();
    }
  );

  it("retains capture ownership after timeout and ignores a late provider success", async () => {
    vi.useFakeTimers();
    try {
      let complete!: (result: { success: boolean; imageId: string }) => void;
      const takeSnapshot = vi.fn(
        () =>
          new Promise<{ success: boolean; imageId: string }>((resolve) => {
            complete = resolve;
          })
      );
      const f = fixture(provider({ takeSnapshot }));
      await readyFinite(f);
      const capture = f.preservation.checkpoint("execution_complete");
      await vi.advanceTimersByTimeAsync(300_001);
      expect(await capture).toMatchObject({ kind: "unknown" });
      expect(f.preservation.mayDispatch()).toBe(true);
      expect(f.preservation.mayAcquire()).toBe(false);
      expect(() => f.preservation.beginGeneration({ ...GENERATION, createdAt: 2_000 })).toThrow(
        "unresolved checkpoint"
      );
      complete({ success: true, imageId: "late" });
      await Promise.resolve();
      expect(f.deps.sandbox.recordSandboxSnapshot).not.toHaveBeenCalled();
      expect(await f.preservation.checkpoint("retry")).toMatchObject({
        kind: "skipped",
        reason: "capture_owned",
      });
      await f.preservation.request("final");
      expect(f.store.value?.phase).toBe("unknown");
      expect(f.preservation.mayDispatch()).toBe(false);
      expect(takeSnapshot).toHaveBeenCalledOnce();
    } finally {
      vi.useRealTimers();
    }
  });

  it("recognizes a lost capture after restart without blocking nondestructive ordinary work", async () => {
    const f = fixture(
      provider({
        takeSnapshot: vi.fn(async () => {
          throw new Error("lost");
        }),
      })
    );
    await readyFinite(f);
    await f.preservation.checkpoint("execution_complete");
    const op = f.store.value!.checkpoint!;
    f.store.write({ ...f.store.value!, checkpoint: { ...op, phase: "capturing" } });
    const restarted = new SandboxPreservation(f.deps);
    expect(restarted.mayAcquire()).toBe(false);
    await restarted.handleAlarm();
    expect(f.store.value?.checkpoint?.phase).toBe("unknown");
    expect(restarted.mayDispatch()).toBe(true);
    expect(f.deps.provider.takeSnapshot).toHaveBeenCalledOnce();
  });

  it("releases a reservation only when deadline scheduling fails before any provider call", async () => {
    const takeSnapshot = vi.fn(async () => ({ success: true, imageId: "image" }));
    const f = fixture(provider({ takeSnapshot }));
    await readyFinite(f);
    f.deps.alarm.schedule.mockRejectedValueOnce(new Error("alarm failure"));
    expect(await f.preservation.checkpoint("execution_complete")).toMatchObject({ kind: "failed" });
    expect(takeSnapshot).not.toHaveBeenCalled();
    expect(f.preservation.mayAcquire()).toBe(true);
    expect(await f.preservation.checkpoint("retry")).toMatchObject({ kind: "completed" });
  });

  it("does not start capture after an alarm await consumes its absolute deadline", async () => {
    const takeSnapshot = vi.fn(async () => ({ success: true, imageId: "image" }));
    const f = fixture(provider({ takeSnapshot }));
    await readyFinite(f);
    f.deps.alarm.schedule.mockImplementationOnce(async () => {
      f.setNow(400_000);
    });
    expect(await f.preservation.checkpoint("execution_complete")).toEqual({
      kind: "failed",
      reason: "checkpoint_deadline_elapsed_before_capture",
    });
    expect(takeSnapshot).not.toHaveBeenCalled();
    expect(f.preservation.mayAcquire()).toBe(true);
  });

  it("does not publish a provider result delivered past the absolute deadline", async () => {
    const f = fixture(
      provider({
        takeSnapshot: vi.fn(async () => {
          f.setNow(400_000);
          return { success: true, imageId: "late-image" };
        }),
      })
    );
    await readyFinite(f);
    expect(await f.preservation.checkpoint("execution_complete")).toMatchObject({
      kind: "unknown",
    });
    expect(f.deps.sandbox.recordSandboxSnapshot).not.toHaveBeenCalled();
  });

  it("rearms a capture deadline when an earlier alarm is delivered", async () => {
    let complete!: (result: { success: boolean; imageId: string }) => void;
    const takeSnapshot = vi.fn(
      () =>
        new Promise<{ success: boolean; imageId: string }>((resolve) => {
          complete = resolve;
        })
    );
    const f = fixture(provider({ takeSnapshot }));
    await readyFinite(f);
    const capture = f.preservation.checkpoint("execution_complete");
    await vi.waitFor(() => expect(takeSnapshot).toHaveBeenCalledOnce());
    f.deps.alarm.schedule.mockClear();
    await f.preservation.handleAlarm();
    expect(f.deps.alarm.schedule).toHaveBeenCalledWith(400_000);
    complete({ success: true, imageId: "image" });
    await capture;
  });

  it("holds legacy retirement rather than destroying a source whose final capture was skipped", async () => {
    const f = fixture();
    expect(f.preservation.mayDispatch()).toBe(true);
    expect(await f.preservation.request("inactivity_timeout")).toBe(true);
    expect(f.store.value).toMatchObject({
      phase: "unknown",
      providerObjectId: "provider-object-1",
    });
    expect(f.preservation.mayDispatch()).toBe(false);
    expect(f.preservation.mayAcquire()).toBe(false);
    expect(f.deps.completePreservation).not.toHaveBeenCalled();
  });

  it("clears the legacy marker only after explicit saved-state recovery confirms source retirement", async () => {
    const stopSandbox = vi.fn(async () => ({ success: true }));
    const f = fixture(provider({ stopSandbox }));
    await readyFinite(f);
    f.store.write({
      ...f.store.value!,
      checkpointInFlight: true,
      receipt: {
        kind: "snapshot",
        provider: "modal",
        artifactId: "previous-saved",
        runtimeVersion: "runtime-1",
        savedAtMs: 50_000,
      },
    });
    expect(f.preservation.isHolding()).toBe(true);
    expect(stopSandbox).not.toHaveBeenCalled();
    await f.preservation.recover("restore_saved");
    expect(stopSandbox).toHaveBeenCalledOnce();
    expect(f.store.value?.checkpointInFlight).toBeUndefined();
    expect(f.preservation.recoveryReceipt()?.artifactId).toBe("previous-saved");
  });

  it("does not downgrade a committed checkpoint when broadcast fails", async () => {
    const f = fixture(
      provider({ takeSnapshot: vi.fn(async () => ({ success: true, imageId: "image" })) })
    );
    await readyFinite(f);
    f.deps.messenger.broadcast.mockImplementation(() => {
      throw new Error("socket failed");
    });
    expect(await f.preservation.checkpoint("execution_complete")).toMatchObject({
      kind: "completed",
    });
    expect(f.store.value?.checkpoint?.phase).toBe("completed");
  });

  it("closes ordinary admission if a supposedly nondestructive provider stops the source", async () => {
    const f = fixture(
      provider({
        takeSnapshot: vi.fn(async () => ({ success: true, imageId: "image", sourceStopped: true })),
      })
    );
    await readyFinite(f);
    expect(await f.preservation.checkpoint("execution_complete")).toMatchObject({
      kind: "unknown",
    });
    expect(f.preservation.mayDispatch()).toBe(false);
    expect(f.deps.sandbox.recordSandboxSnapshot).not.toHaveBeenCalled();
  });

  it.each(["missing", "legacy_status", "legacy_marker"])(
    "fails closed for %s checkpoint ownership",
    async (mode) => {
      const takeSnapshot = vi.fn();
      const f = fixture(provider({ takeSnapshot }));
      if (mode === "legacy_status") f.sandboxRow.status = "snapshotting";
      if (mode === "legacy_marker") {
        await readyFinite(f);
        f.store.write({ ...f.store.value!, checkpointInFlight: true });
      }
      expect(await f.preservation.checkpoint("execution_complete")).toMatchObject({
        kind: "skipped",
      });
      expect(takeSnapshot).not.toHaveBeenCalled();
      expect(f.preservation.isHolding()).toBe(mode !== "missing");
    }
  );

  it("routes destructive capture through final preparation and never falls through", async () => {
    const p = provider({ takeSnapshot: vi.fn() });
    p.capabilities.snapshotStopsSandbox = true;
    const f = fixture(p);
    await readyFinite(f);
    expect(await f.preservation.checkpoint("execution_complete")).toMatchObject({
      kind: "skipped",
      reason: "final_preservation_requested",
    });
    expect(f.store.value?.phase).toBe("draining");
    expect(p.takeSnapshot).not.toHaveBeenCalled();
    expect(await f.preservation.checkpoint("again")).toMatchObject({
      kind: "skipped",
      reason: "preservation_held",
    });
    expect(p.takeSnapshot).not.toHaveBeenCalled();
  });
});
