import { beforeEach, describe, expect, it, vi } from "vitest";
import { SandboxPreservation } from "./sandbox-preservation";
import {
  GENERATION,
  fixture,
  provider,
  readyFinite,
  readyWithoutDeadline,
  preparedEvent,
} from "./sandbox-preservation.test-fixtures";
describe("SandboxPreservation", () => {
  beforeEach(() => vi.restoreAllMocks());

  it("claims heartbeat checkpoint retirement before yielding and rejects competing final ownership", async () => {
    let stopped!: (result: { success: boolean }) => void;
    const stopSandbox = vi.fn(
      () =>
        new Promise<{ success: boolean }>((resolve) => {
          stopped = resolve;
        })
    );
    const f = fixture(
      provider({
        takeSnapshot: vi.fn(async () => ({ success: true, imageId: "heartbeat-image" })),
        stopSandbox,
      })
    );
    await readyFinite(f);
    f.sandboxRow.status = "stale";
    const checkpoint = await f.preservation.checkpoint("heartbeat_timeout");
    expect(checkpoint.kind).toBe("completed");
    if (checkpoint.kind !== "completed") throw new Error("Expected completed capture");
    const retiring = f.preservation.retireHeartbeatCheckpoint(checkpoint.operationId);
    await vi.waitFor(() => expect(stopSandbox).toHaveBeenCalledOnce());
    expect(f.store.value).toMatchObject({ phase: "retiring", operationId: checkpoint.operationId });
    expect(await f.preservation.request("inactivity_timeout")).toBe(false);
    stopped({ success: true });
    await retiring;
    expect(f.store.value).toMatchObject({ phase: "saved", sourceRetired: true });
    expect(f.deps.completePreservation).toHaveBeenCalledOnce();
  });

  it("starts the final stop budget after a checkpoint taking longer than STOP_MS settles", async () => {
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
    await f.preservation.request("final");
    expect(f.store.value).toMatchObject({ phase: "waiting_for_checkpoint", waitByMs: 400_000 });
    expect(f.store.value?.stopByMs).toBeUndefined();
    f.setNow(161_000);
    await f.preservation.handleAlarm();
    expect(f.store.value?.phase).toBe("waiting_for_checkpoint");
    complete({ success: true, imageId: "ordinary" });
    await checkpoint;
    await f.backgroundTasks.at(-1)!();
    expect(f.store.value).toMatchObject({
      phase: "draining",
      stopByMs: 221_000,
      captureByMs: 521_000,
    });
    expect(f.deps.sockets.send).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ type: "prepare_preservation", stopByMs: 221_000 })
    );
  });

  it.each(["restart", "deadline"])(
    "retains the wait hold on %s instead of starting another capture",
    async (mode) => {
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
      // Leave only 10 seconds of waiting before the hard lifetime budget must be reserved.
      f.store.write({ ...f.store.value!, expiresAtMs: 230_000, drainAtMs: 200_000 });
      await f.preservation.request("final");
      expect(f.store.value?.waitByMs).toBe(110_000);
      const owner = mode === "restart" ? new SandboxPreservation(f.deps) : f.preservation;
      if (mode === "deadline") f.setNow(110_000);
      await owner.handleAlarm();
      expect(f.store.value?.phase).toBe("unknown");
      expect(owner.mayDispatch()).toBe(false);
      complete({ success: true, imageId: "ordinary" });
      await checkpoint;
      expect(f.store.value?.phase).toBe("unknown");
      expect(takeSnapshot).toHaveBeenCalledOnce();
    }
  );

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
    expect(f.deps.messenger.broadcast).toHaveBeenCalledWith({ type: "sandbox_access_changed" });
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
        ...f.deps.session.getSession(),
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
    f.deps.messages.getProcessingMessage.mockReturnValue({
      id: "message-1",
      author_id: "user-1",
      content: "prompt",
      source: "web",
      model: null,
      reasoning_effort: null,
      attachments: null,
      callback_context: null,
      client_request_id: null,
      request_fingerprint: null,
      autofix_feedback_key: null,
      autofix_pr_key: null,
      origin_context: null,
      status: "processing",
      error_message: null,
      stop_confirmation_deadline: null,
      created_at: 1_000,
      started_at: 2_000,
      completed_at: null,
    });
    f.deps.failures.record.mockReturnValue({
      event: {
        type: "execution_complete",
        messageId: "message-1",
        success: false,
        sandboxId: GENERATION.sandboxId,
        timestamp: 100,
      },
      completion: {
        messageId: "message-1",
        messageCreatedAt: 1_000,
        messageStartedAt: 2_000,
        completedAt: 100_000,
        status: "failed",
      },
    });
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

    const restarted = new SandboxPreservation({ ...f.deps, store: f.store });
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

  it.each(["snapshot", "retained"] as const)(
    "keeps a scheduling failure before %s provider I/O retryable",
    async (mode) => {
      const takeSnapshot = vi.fn(async () => ({
        success: true,
        imageId: "final-image",
        sourceStopped: true,
      }));
      const stopSandbox = vi.fn(async () => ({ success: true }));
      const f = fixture(
        provider({
          takeSnapshot,
          stopSandbox,
          capabilities: {
            ...provider().capabilities,
            supportsSnapshots: mode === "snapshot",
            supportsPersistentResume: mode === "retained",
          },
        })
      );
      await readyFinite(f);
      await f.preservation.request("sandbox_lifetime_expiring");
      const firstOperation = f.store.value!.operationId;
      f.preservation.prepared(preparedEvent(f.store.value!));
      f.deps.alarm.schedule.mockRejectedValueOnce(new Error("Temporary alarm failure"));

      await f.preservation.handleAlarm();

      expect(f.store.value?.phase).toBe("failed");
      expect(takeSnapshot).not.toHaveBeenCalled();
      expect(stopSandbox).not.toHaveBeenCalled();
      await f.preservation.recover("retry");
      expect(f.store.value?.phase).toBe("draining");
      expect(f.store.value?.operationId).not.toBe(firstOperation);
      f.preservation.prepared(preparedEvent(f.store.value!));
      await f.preservation.handleAlarm();
      expect(f.store.value?.phase).toBe("saved");
      expect(mode === "snapshot" ? takeSnapshot : stopSandbox).toHaveBeenCalledOnce();
    }
  );

  it("keeps deadline exhaustion during alarm scheduling retryable before provider invocation", async () => {
    const takeSnapshot = vi.fn();
    const f = fixture(provider({ takeSnapshot }));
    await readyFinite(f);
    await f.preservation.request("sandbox_lifetime_expiring");
    f.preservation.prepared(preparedEvent(f.store.value!));
    f.deps.alarm.schedule.mockImplementationOnce(async () => {
      f.setNow(f.store.value!.captureByMs!);
    });
    await f.preservation.handleAlarm();
    expect(takeSnapshot).not.toHaveBeenCalled();
    expect(f.store.value?.phase).toBe("failed");
    await f.preservation.recover("retry");
    expect(f.store.value?.phase).toBe("draining");
  });

  it("retains an unknown hold after provider invocation throws", async () => {
    const takeSnapshot = vi.fn(async () => {
      throw new Error("Connection lost after request");
    });
    const f = fixture(provider({ takeSnapshot }));
    await readyFinite(f);
    await f.preservation.request("sandbox_lifetime_expiring");
    f.preservation.prepared(preparedEvent(f.store.value!));
    await f.preservation.handleAlarm();
    expect(takeSnapshot).toHaveBeenCalledOnce();
    expect(f.store.value?.phase).toBe("unknown");
    await expect(f.preservation.recover("retry")).rejects.toThrow("cannot be retried safely");
    expect(takeSnapshot).toHaveBeenCalledOnce();
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

    const restarted = new SandboxPreservation(f.deps);
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
