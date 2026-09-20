import { beforeEach, describe, expect, it, vi } from "vitest";
import type { SandboxEvent } from "@open-inspect/shared/types/sandbox-events";
import type { SandboxProvider } from "../sandbox/provider";
import { SandboxPreservation } from "./sandbox-preservation";
import type { PreservationRecord, PreservationStore } from "./sandbox-preservation-repository";

const GENERATION = { sandboxId: "sandbox-1", createdAt: 1_000 };

class MemoryStore implements PreservationStore {
  value: PreservationRecord | null = null;
  read() {
    return this.value;
  }
  write(record: PreservationRecord) {
    this.value = structuredClone(record);
  }
}

function provider(overrides: Partial<SandboxProvider> = {}): SandboxProvider {
  return {
    name: "modal",
    capabilities: {
      supportsSandboxTimeout: true,
      supportsSnapshots: true,
      snapshotStopsSandbox: false,
      supportsRestore: true,
      supportsPersistentResume: false,
      supportsExplicitStop: true,
    },
    ...overrides,
  } as SandboxProvider;
}

function fixture(providerValue = provider()) {
  let now = 100_000;
  const store = new MemoryStore();
  const calls: string[] = [];
  const backgroundTasks: Array<() => Promise<void>> = [];
  const socket = {};
  const sandboxRow = {
    modal_sandbox_id: GENERATION.sandboxId,
    modal_object_id: "provider-object-1" as string | null,
    created_at: GENERATION.createdAt,
    runtime_version: "runtime-1",
    status: "ready",
  };
  const deps = {
    store,
    provider: providerValue,
    sandbox: {
      getSandbox: vi.fn(() => sandboxRow),
      recordSandboxSnapshot: vi.fn(() => {
        calls.push("snapshot-recorded");
        return true;
      }),
    },
    session: {
      getSession: vi.fn(() => ({
        id: "session-1",
        session_name: "external-session-1",
        sandbox_settings: JSON.stringify({ finalSnapshotBufferMs: 600_000 }),
      })),
      transaction: vi.fn((fn: () => unknown) => fn()),
    },
    messages: {
      getProcessingMessage: vi.fn<() => { id: string } | null>(() => null),
    },
    failures: { record: vi.fn(), deliver: vi.fn() },
    messenger: {
      broadcast: vi.fn((message: { type: string; preservation?: { phase: string } }) => {
        if (message.type === "sandbox_preservation" && message.preservation) {
          calls.push(`phase:${message.preservation.phase}`);
        }
      }),
    },
    sockets: {
      getSandboxSocket: vi.fn(() => socket),
      send: vi.fn(),
    },
    alarm: { schedule: vi.fn<() => Promise<void>>(async () => {}) },
    background: {
      submit: vi.fn((task: () => Promise<void>) => backgroundTasks.push(task)),
    },
    processQueue: vi.fn(async () => undefined),
    reconcileStatus: vi.fn(async () => undefined),
    completePreservation: vi.fn(() => {
      calls.push("sandbox-stopped", "access-retired");
      return true;
    }),
    now: () => now,
  };
  const preservation = new SandboxPreservation(deps as never);
  return {
    preservation,
    deps,
    store,
    calls,
    backgroundTasks,
    sandboxRow,
    setNow(value: number) {
      now = value;
    },
  };
}

async function readyFinite(f: ReturnType<typeof fixture>, expiresAtMs = 1_300_000) {
  f.preservation.beginGeneration(GENERATION);
  await f.preservation.started(GENERATION, {
    kind: "finite",
    expiresAtMs,
    observedAtMs: 100_000,
    source: "provider",
  });
  f.preservation.runtimeReady(1);
  f.preservation.generationReady({
    type: "sandbox_generation_ready",
    generation: GENERATION,
    sandboxId: GENERATION.sandboxId,
    timestamp: 1,
  });
}

async function readyWithoutDeadline(f: ReturnType<typeof fixture>) {
  f.preservation.beginGeneration(GENERATION);
  await f.preservation.started(GENERATION, { kind: "none", observedAtMs: 100_000 });
  f.preservation.runtimeReady(1);
  f.preservation.generationReady({
    type: "sandbox_generation_ready",
    generation: GENERATION,
    sandboxId: GENERATION.sandboxId,
    timestamp: 1,
  });
}

function preparedEvent(
  state: PreservationRecord
): Extract<SandboxEvent, { type: "preservation_prepared" }> {
  return {
    type: "preservation_prepared",
    operationId: state.operationId!,
    generation: GENERATION,
    executionStopped: true,
    sandboxId: GENERATION.sandboxId,
    timestamp: 1,
  };
}

describe("SandboxPreservation", () => {
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

  it.each(["stopped", "stale", "failed"])(
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
    const restarted = new SandboxPreservation(f.deps as never);
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

  it("re-reads completed checkpoint ownership after the final alarm scheduling await", async () => {
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
    let arm!: () => void;
    f.deps.alarm.schedule.mockImplementationOnce(
      () =>
        new Promise<void>((resolve) => {
          arm = resolve;
        })
    );
    const final = f.preservation.request("final");
    complete({ success: true, imageId: "checkpoint" });
    await capture;
    arm();
    await final;
    expect(f.store.value).toMatchObject({ phase: "draining", checkpoint: { phase: "completed" } });
    expect(f.deps.sockets.send).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ type: "prepare_preservation" })
    );
  });

  it("derives one absolute stop/capture/retire budget and sends a correlated command", async () => {
    const f = fixture();
    await readyFinite(f);
    expect(f.preservation.mayDispatch()).toBe(true);

    await f.preservation.request("sandbox_lifetime_expiring");

    expect(f.store.value).toMatchObject({
      phase: "draining",
      stopByMs: 160_000,
      captureByMs: 460_000,
      retireByMs: 1_270_000,
    });
    expect(f.deps.sockets.send).toHaveBeenLastCalledWith(
      expect.anything(),
      expect.objectContaining({
        type: "prepare_preservation",
        operationId: f.store.value!.operationId,
        generation: GENERATION,
        stopByMs: 160_000,
      })
    );
    expect(f.preservation.mayDispatch()).toBe(false);
  });

  it("requires a matching generation acknowledgement and ignores a late generation", async () => {
    const f = fixture();
    f.preservation.beginGeneration(GENERATION);
    await f.preservation.started(GENERATION, {
      kind: "none",
      observedAtMs: 100_000,
    });
    f.preservation.runtimeReady(1);
    expect(f.preservation.mayDispatch()).toBe(false);

    f.preservation.generationReady({
      type: "sandbox_generation_ready",
      generation: { ...GENERATION, createdAt: 999 },
      sandboxId: GENERATION.sandboxId,
      timestamp: 1,
    });
    expect(f.preservation.mayDispatch()).toBe(false);

    f.preservation.generationReady({
      type: "sandbox_generation_ready",
      generation: GENERATION,
      sandboxId: GENERATION.sandboxId,
      timestamp: 1,
    });
    expect(f.preservation.mayDispatch()).toBe(true);
  });

  it.each([
    { bufferMs: 300_000, alarmDelayMs: 0, captureMs: 180_000 },
    { bufferMs: 300_000, alarmDelayMs: 30_000, captureMs: 150_000 },
    { bufferMs: 600_000, alarmDelayMs: 0, captureMs: 300_000 },
  ])(
    "preserves within buffer $bufferMs with alarm delay $alarmDelayMs",
    async ({ bufferMs, alarmDelayMs, captureMs }) => {
      const takeSnapshot = vi.fn(async () => ({
        success: true,
        imageId: "final-image",
        sourceStopped: true,
      }));
      const f = fixture(provider({ takeSnapshot }));
      f.deps.session.getSession.mockReturnValue({
        id: "session-1",
        session_name: "external-session-1",
        sandbox_settings: JSON.stringify({ finalSnapshotBufferMs: bufferMs }),
      });
      const expiresAtMs = 1_300_000;
      await readyFinite(f, expiresAtMs);
      expect(f.store.value?.drainAtMs).toBe(expiresAtMs - bufferMs);
      const alarmAtMs = expiresAtMs - bufferMs + alarmDelayMs;
      f.setNow(alarmAtMs);
      await f.preservation.handleAlarm();

      const state = f.store.value!;
      expect(state.phase).toBe("draining");
      expect(state.stopByMs).toBe(alarmAtMs + 60_000);
      expect(state.captureByMs).toBe(state.stopByMs! + captureMs);
      expect(state.captureByMs).toBeLessThanOrEqual(expiresAtMs - 60_000);
      expect(state.retireByMs).toBe(expiresAtMs - 30_000);

      f.setNow(state.stopByMs! - 1);
      f.preservation.prepared(preparedEvent(state));
      await f.preservation.handleAlarm();
      expect(takeSnapshot).toHaveBeenCalledWith(
        expect.objectContaining({ deadlineAtMs: state.captureByMs })
      );
      expect(f.store.value?.phase).toBe("saved");
    }
  );

  it("pins an active generation to the provider that created it", async () => {
    const f = fixture(provider({ name: "modal" }));
    await readyFinite(f);
    f.store.write({ ...f.store.value!, provider: "e2b" });

    expect(f.preservation.mayDispatch()).toBe(false);
    expect(f.store.value).toMatchObject({
      phase: "unknown",
      error: expect.stringContaining("provider changed"),
    });
  });

  it("allows a fresh-spawn retry after startup fails with a prior provider handle", () => {
    const f = fixture();
    f.sandboxRow.status = "failed";
    f.preservation.beginGeneration(GENERATION);

    expect(f.store.value).toMatchObject({
      phase: "running",
      provider: "modal",
      providerObjectId: null,
      lifetimeKind: "unknown",
    });
    expect(f.preservation.mayDispatch()).toBe(true);
  });

  it("serializes final preparation behind an ordinary checkpoint", async () => {
    let complete!: (result: { success: boolean; imageId: string }) => void;
    const takeSnapshot = vi.fn(
      () =>
        new Promise<{ success: boolean; imageId: string }>((resolve) => {
          complete = resolve;
        })
    );
    const f = fixture(provider({ takeSnapshot }));
    await readyFinite(f);
    const checkpoint = f.preservation.checkpoint("execution_complete");
    await vi.waitFor(() => expect(takeSnapshot).toHaveBeenCalledOnce());
    expect(f.preservation.mayDispatch()).toBe(true);
    expect(f.preservation.mayAcquire()).toBe(false);

    await f.preservation.request("sandbox_lifetime_expiring");
    expect(f.deps.sockets.send).not.toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ type: "prepare_preservation" })
    );

    complete({ success: true, imageId: "checkpoint" });
    expect(await checkpoint).toMatchObject({ kind: "completed" });
    expect(f.preservation.mayDispatch()).toBe(false);
    expect(f.store.value?.receipt).toBeUndefined();
    await f.backgroundTasks.at(-1)!();
    expect(f.deps.sockets.send).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ type: "prepare_preservation" })
    );
    // The ordinary checkpoint must not become the final receipt. Preparation
    // starts a distinct provider capture with its own result.
    f.preservation.prepared(preparedEvent(f.store.value!));
    const finalCapture = f.preservation.handleAlarm();
    await vi.waitFor(() => expect(takeSnapshot).toHaveBeenCalledTimes(2));
    complete({ success: true, imageId: "final-image" });
    await finalCapture;
    expect(f.store.value?.receipt?.artifactId).toBe("final-image");
    expect(f.store.value?.checkpoint).toMatchObject({ phase: "completed", imageId: "checkpoint" });
  });

  it("settles the active message once when duplicate preservation requests race", async () => {
    const f = fixture();
    f.deps.messages.getProcessingMessage.mockReturnValue({ id: "message-1" });
    f.deps.failures.record.mockReturnValue({ id: "failure-1" });
    await readyFinite(f);

    await f.preservation.request("sandbox_lifetime_expiring");
    await f.preservation.request("sandbox_lifetime_expiring");

    expect(f.deps.failures.record).toHaveBeenCalledOnce();
    expect(f.deps.failures.record).toHaveBeenCalledWith(
      "message-1",
      "sandbox_lifetime_expiring",
      100_000,
      "processing"
    );
    expect(f.deps.failures.deliver).toHaveBeenCalledOnce();
    expect(f.store.value?.messageId).toBe("message-1");
  });

  it("ignores duplicate prepared evidence after the durable phase transition", async () => {
    const f = fixture();
    await readyFinite(f);
    await f.preservation.request("sandbox_lifetime_expiring");
    const event = preparedEvent(f.store.value!);
    f.backgroundTasks.length = 0;

    f.preservation.prepared(event);
    f.preservation.prepared(event);

    expect(f.store.value?.phase).toBe("prepared");
    expect(f.backgroundTasks).toHaveLength(1);
  });

  it("replays the same correlated preparation after restart while draining", async () => {
    const f = fixture();
    await readyFinite(f);
    await f.preservation.request("sandbox_lifetime_expiring");
    const operationId = f.store.value!.operationId;
    f.deps.sockets.send.mockClear();

    const restarted = new SandboxPreservation({ ...f.deps, store: f.store } as never);
    expect(await restarted.handleAlarm()).toBe(true);

    expect(f.deps.sockets.send).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ type: "prepare_preservation", operationId })
    );
  });

  it("marks an in-flight provider capture unknown after coordinator restart", async () => {
    const f = fixture();
    f.store.write({
      phase: "capturing",
      generation: GENERATION,
      providerObjectId: "provider-object-1",
      lifetimeKind: "finite",
      expiresAtMs: 1_300_000,
      drainAtMs: 700_000,
      generationReady: true,
      protocolVersion: 1,
      operationId: "operation-1",
      stopByMs: 160_000,
      captureByMs: 460_000,
      retireByMs: 1_270_000,
    });

    expect(await f.preservation.handleAlarm()).toBe(true);
    expect(f.store.value).toMatchObject({
      phase: "unknown",
      error: expect.stringContaining("provider result is unknown"),
    });
    expect(f.deps.provider.takeSnapshot).toBeUndefined();
  });

  it("commits a snapshot receipt before retiring an independently captured source", async () => {
    const f = fixture(
      provider({
        takeSnapshot: vi.fn(async () => {
          f.calls.push("provider-snapshot");
          f.sandboxRow.runtime_version = "later-runtime-report";
          return { success: true, imageId: "image-1", sourceStopped: false };
        }),
        stopSandbox: vi.fn(async () => {
          f.calls.push("provider-stop");
          expect(f.store.value).toMatchObject({
            phase: "retiring",
            receipt: { kind: "snapshot", artifactId: "image-1" },
          });
          return { success: true };
        }),
      })
    );
    await readyFinite(f);
    await f.preservation.request("sandbox_lifetime_expiring");
    f.preservation.prepared(preparedEvent(f.store.value!));

    await f.preservation.handleAlarm();

    expect(f.store.value).toMatchObject({
      phase: "saved",
      receipt: {
        kind: "snapshot",
        artifactId: "image-1",
        provider: "modal",
        runtimeVersion: "runtime-1",
      },
    });
    expect(f.calls.indexOf("phase:retiring")).toBeLessThan(f.calls.indexOf("snapshot-recorded"));
    expect(f.calls.indexOf("snapshot-recorded")).toBeLessThan(f.calls.indexOf("provider-stop"));
    expect(f.calls).toContain("access-retired");
  });

  it("reconciles a committed receipt by retiring after coordinator restart", async () => {
    const stopSandbox = vi.fn(async () => ({ success: true }));
    const f = fixture(provider({ stopSandbox }));
    f.store.write({
      phase: "retiring",
      generation: GENERATION,
      providerObjectId: "provider-object-1",
      lifetimeKind: "finite",
      expiresAtMs: 1_300_000,
      drainAtMs: 700_000,
      generationReady: true,
      protocolVersion: 1,
      operationId: "operation-1",
      reason: "sandbox_lifetime_expiring",
      stopByMs: 160_000,
      captureByMs: 460_000,
      retireByMs: 1_270_000,
      receipt: {
        kind: "snapshot",
        artifactId: "image-1",
        provider: "modal",
        savedAtMs: 150_000,
        runtimeVersion: "runtime-1",
      },
      savedAtMs: 150_000,
    });

    expect(await f.preservation.handleAlarm()).toBe(true);
    expect(stopSandbox).toHaveBeenCalledOnce();
    expect(f.store.value?.phase).toBe("saved");
  });

  it.each(["e2b", "daytona"])(
    "uses retained-object preservation for %s without fabricating a snapshot id",
    async (name) => {
      const stopSandbox = vi.fn(async () => ({ success: true }));
      const takeSnapshot = vi.fn();
      const f = fixture(
        provider({
          name,
          capabilities: {
            supportsSandboxTimeout: name === "e2b",
            supportsSnapshots: false,
            supportsRestore: false,
            supportsPersistentResume: true,
            supportsExplicitStop: true,
          },
          stopSandbox,
          takeSnapshot,
        })
      );
      if (name === "daytona") await readyWithoutDeadline(f);
      else await readyFinite(f);
      await f.preservation.request("sandbox_lifetime_expiring");
      f.preservation.prepared(preparedEvent(f.store.value!));

      await f.preservation.handleAlarm();

      expect(stopSandbox).toHaveBeenCalledTimes(1);
      expect(takeSnapshot).not.toHaveBeenCalled();
      expect(f.store.value).toMatchObject({
        phase: "saved",
        receipt: {
          kind: "retained",
          artifactId: "provider-object-1",
          provider: name,
        },
      });
      expect(f.deps.sandbox.recordSandboxSnapshot).not.toHaveBeenCalled();
    }
  );

  it("does not retire a destructive-snapshot source twice", async () => {
    const stopSandbox = vi.fn();
    const f = fixture(
      provider({
        name: "vercel",
        takeSnapshot: vi.fn(async () => ({
          success: true,
          imageId: "snapshot-1",
          sourceStopped: true,
        })),
        stopSandbox,
      })
    );
    await readyFinite(f);
    await f.preservation.request("sandbox_lifetime_expiring");
    f.preservation.prepared(preparedEvent(f.store.value!));

    await f.preservation.handleAlarm();

    expect(stopSandbox).not.toHaveBeenCalled();
    expect(f.store.value?.phase).toBe("saved");
  });

  it("prefers an independent checkpoint for OpenComputer without a hard expiry", async () => {
    const takeSnapshot = vi.fn(async () => ({
      success: true,
      imageId: "checkpoint-1",
      sourceStopped: false,
    }));
    const stopSandbox = vi.fn(async () => ({ success: true }));
    const f = fixture(
      provider({
        name: "opencomputer",
        capabilities: {
          supportsSandboxTimeout: true,
          supportsSnapshots: true,
          supportsRestore: true,
          supportsPersistentResume: true,
          supportsExplicitStop: true,
        },
        takeSnapshot,
        stopSandbox,
      })
    );
    await readyWithoutDeadline(f);
    await f.preservation.request("sandbox_lifetime_expiring");
    f.preservation.prepared(preparedEvent(f.store.value!));

    await f.preservation.handleAlarm();

    expect(takeSnapshot).toHaveBeenCalledOnce();
    expect(stopSandbox).toHaveBeenCalledOnce();
    expect(f.store.value).toMatchObject({
      phase: "saved",
      receipt: { kind: "snapshot", artifactId: "checkpoint-1" },
    });
  });

  it("drops a late capture result after the sandbox generation changes", async () => {
    let resolveCapture!: (value: {
      success: true;
      imageId: string;
      sourceStopped: boolean;
    }) => void;
    const capture = new Promise<{
      success: true;
      imageId: string;
      sourceStopped: boolean;
    }>((resolve) => {
      resolveCapture = resolve;
    });
    const f = fixture(provider({ takeSnapshot: vi.fn(() => capture) }));
    await readyFinite(f);
    await f.preservation.request("sandbox_lifetime_expiring");
    f.preservation.prepared(preparedEvent(f.store.value!));
    const advancing = f.preservation.handleAlarm();
    await vi.waitFor(() => expect(f.store.value?.phase).toBe("capturing"));

    const replacement = { sandboxId: "sandbox-2", createdAt: 2_000 };
    f.sandboxRow.modal_sandbox_id = replacement.sandboxId;
    f.sandboxRow.created_at = replacement.createdAt;
    f.preservation.beginGeneration(replacement);
    resolveCapture({ success: true, imageId: "late-image", sourceStopped: false });
    await advancing;

    expect(f.store.value).toMatchObject({ phase: "running", generation: replacement });
    expect(f.deps.sandbox.recordSandboxSnapshot).not.toHaveBeenCalled();
  });

  it("times out an ambiguous capture without retiring the source", async () => {
    vi.useFakeTimers();
    try {
      const stopSandbox = vi.fn();
      const f = fixture(
        provider({
          takeSnapshot: vi.fn(() => new Promise<never>(() => undefined)),
          stopSandbox,
        })
      );
      await readyFinite(f);
      await f.preservation.request("sandbox_lifetime_expiring");
      f.preservation.prepared(preparedEvent(f.store.value!));
      f.setNow(f.store.value!.captureByMs! - 1);

      const advancing = f.preservation.handleAlarm();
      await vi.advanceTimersByTimeAsync(1);
      await advancing;

      expect(f.store.value).toMatchObject({
        phase: "unknown",
        error: expect.stringContaining("deadline exceeded"),
      });
      expect(stopSandbox).not.toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
    }
  });

  it("retries only a confirmed pre-capture failure with a new operation", async () => {
    const f = fixture();
    await readyFinite(f);
    await f.preservation.request("sandbox_lifetime_expiring");
    const firstOperation = f.store.value!.operationId;
    f.preservation.prepared({
      ...preparedEvent(f.store.value!),
      executionStopped: false,
      error: "execution_stop_unconfirmed",
    });
    expect(f.store.value?.phase).toBe("failed");
    f.deps.sockets.send.mockClear();

    await f.preservation.recover("retry");

    expect(f.store.value).toMatchObject({ phase: "draining", error: undefined });
    expect(f.store.value?.operationId).not.toBe(firstOperation);
    expect(f.deps.sockets.send).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        type: "prepare_preservation",
        operationId: f.store.value?.operationId,
      })
    );
  });

  it("refuses to repeat capture after an unknown provider result", async () => {
    const f = fixture();
    await readyFinite(f);
    await f.preservation.request("sandbox_lifetime_expiring");
    f.store.write({ ...f.store.value!, phase: "unknown", error: "capture outcome unknown" });
    f.deps.provider.takeSnapshot = vi.fn();

    await expect(f.preservation.recover("retry")).rejects.toThrow(
      "unknown provider result cannot be retried"
    );
    expect(f.deps.provider.takeSnapshot).not.toHaveBeenCalled();
    expect(f.store.value?.phase).toBe("unknown");
  });

  it("retires an unexpired source before restoring the last saved receipt", async () => {
    const stopSandbox = vi.fn(async () => ({ success: true }));
    const f = fixture(provider({ stopSandbox }));
    await readyFinite(f);
    f.store.write({
      ...f.store.value!,
      phase: "unknown",
      error: "capture outcome unknown",
      receipt: {
        kind: "snapshot",
        artifactId: "last-good-image",
        provider: "modal",
        savedAtMs: 50_000,
        runtimeVersion: "runtime-1",
      },
    });
    f.deps.background.submit.mockClear();

    await f.preservation.recover("restore_saved");

    expect(stopSandbox).toHaveBeenCalledOnce();
    expect(f.store.value).toMatchObject({
      phase: "saved",
      receipt: { artifactId: "last-good-image" },
    });
    expect(f.deps.background.submit).toHaveBeenCalledWith(expect.any(Function), {
      name: "message_queue.process",
    });
  });

  it("retains preflight retirement proof across restart without automatically retrying", async () => {
    const f = fixture();
    await readyFinite(f);
    f.store.write({
      ...f.store.value!,
      phase: "saved",
      receipt: {
        kind: "snapshot",
        artifactId: "saved-image",
        provider: "modal",
        savedAtMs: 50_000,
        runtimeVersion: "runtime-1",
      },
    });
    const next = { ...GENERATION, createdAt: GENERATION.createdAt + 1 };
    f.sandboxRow.created_at = next.createdAt;
    f.preservation.beginGeneration(next);
    f.preservation.restoreFailed("preflight failed", next);

    const restarted = new SandboxPreservation(f.deps as never);
    expect(restarted.isHolding()).toBe(true);
    expect(restarted.mayDispatch()).toBe(false);
    expect(restarted.recoveryReceipt()).toBeUndefined();
    await restarted.recover("restore_saved");
    expect(restarted.recoveryReceipt()?.artifactId).toBe("saved-image");
    expect(f.store.value?.sourceRetired).toBe(true);
  });

  it("ignores failed restore publication and rejects startup from a superseded generation", async () => {
    const f = fixture();
    await readyFinite(f);
    f.store.write({
      ...f.store.value!,
      receipt: {
        kind: "snapshot",
        artifactId: "saved-image",
        provider: "modal",
        savedAtMs: 50_000,
        runtimeVersion: "runtime-1",
      },
    });
    const next = { ...GENERATION, createdAt: GENERATION.createdAt + 1 };
    f.sandboxRow.created_at = next.createdAt;
    f.preservation.beginGeneration(next);
    const state = structuredClone(f.store.value);

    f.preservation.restoreFailed("late provider failure", GENERATION);
    expect(() => f.preservation.restoreStarting(GENERATION)).toThrow("superseded");
    expect(f.store.value).toEqual(state);
  });

  it("keeps provider ownership but holds dispatch for an explicit unknown lifetime", async () => {
    const f = fixture();
    f.preservation.beginGeneration(GENERATION);
    await f.preservation.started(GENERATION, {
      kind: "unknown",
      observedAtMs: 100_000,
      reason: "metadata unavailable",
    });
    expect(f.store.value).toMatchObject({
      phase: "unknown",
      providerObjectId: "provider-object-1",
      sourceRetired: false,
    });
    expect(f.preservation.mayDispatch()).toBe(false);
  });
});
