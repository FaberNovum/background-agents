import { DEFAULT_FINAL_SNAPSHOT_BUFFER_MS } from "@open-inspect/shared/types/integrations";
import type { SandboxEvent } from "@open-inspect/shared/types/sandbox-events";
import {
  sandboxPreservationSchema,
  type SandboxPreservationState,
} from "@open-inspect/shared/types/sandbox-preservation";
import type { AlarmScheduler, BackgroundTasks } from "../platform-ports";
import type { Logger } from "../logger";
import type { SandboxLifetime, SandboxProvider } from "../sandbox/provider";
import { parsePersistedSandboxSettings } from "../sandbox/settings";
import type { SandboxGeneration, CheckpointOutcome } from "../sandbox/lifecycle/ports";
import type { SandboxPreservationStorage } from "./sandbox-ports";
import type { SessionCoreRepository } from "./session-core-repository";
import type { MessageRepository } from "./message-repository";
import type { MessageFailureService } from "./message-failure-service";
import type { SessionMessenger } from "./messenger";
import type { SessionWebSocketManager } from "./websocket-manager";
import type {
  CheckpointOperation,
  PreservationRecord,
  PreservationStore,
} from "./sandbox-preservation-repository";

const STOP_MS = 60_000;
const CAPTURE_MS = 300_000;
const RETIRE_MS = 30_000;
const MARGIN_MS = 30_000;

class PreservationDeadlineError extends Error {}

interface PreservationDeps {
  store: PreservationStore;
  provider: SandboxProvider;
  sandbox: SandboxPreservationStorage;
  session: Pick<SessionCoreRepository, "getSession" | "transaction">;
  messages: Pick<MessageRepository, "getProcessingMessage">;
  failures: Pick<MessageFailureService, "record" | "deliver">;
  messenger: Pick<SessionMessenger, "broadcast">;
  sockets: Pick<SessionWebSocketManager, "getSandboxSocket" | "send">;
  alarm: Pick<AlarmScheduler, "schedule">;
  background: BackgroundTasks;
  processQueue(): Promise<void>;
  reconcileStatus(): Promise<void>;
  completePreservation(generation: SandboxGeneration, providerObjectId: string | null): boolean;
  now?: () => number;
  log?: Logger;
}

/** One durable owner of planned stopping. Provider side effects never imply a saved receipt. */
export class SandboxPreservation {
  private activeOperation: string | null = null;
  private checkpointOperation: string | null = null;
  private retiringOperation: string | null = null;
  private readonly now: () => number;

  constructor(private readonly deps: PreservationDeps) {
    this.now = deps.now ?? Date.now;
  }

  /** Old labels/markers are evidence of uncertainty, never evidence of readiness. */
  private readState(): PreservationRecord | null {
    const state = this.deps.store.read();
    const row = this.deps.sandbox.getSandbox();
    if (
      row?.modal_sandbox_id &&
      (row.status === "snapshotting" || state?.checkpointInFlight) &&
      (!state || state.phase === "running" || state.phase === "saved")
    ) {
      const legacy: PreservationRecord = {
        ...(state ?? {
          generation: { sandboxId: row.modal_sandbox_id, createdAt: row.created_at },
          provider: this.deps.provider.name,
          providerObjectId: row.modal_object_id,
          lifetimeKind: "unknown" as const,
          expiresAtMs: null,
          drainAtMs: null,
          generationReady: false,
        }),
        phase: "unknown",
        reason: "legacy_checkpoint",
        error:
          "Legacy snapshot ownership is unknown; no readiness or recovery result was inferred.",
      };
      this.publish(legacy);
      return legacy;
    }
    return state;
  }

  /** Unknown capture still owns the source; replacing it must be an explicit recovery decision. */
  mayAcquire(): boolean {
    const state = this.readState();
    return (
      !this.isHolding() && (!this.unresolvedCheckpoint(state) || state?.sourceRetired === true)
    );
  }

  private unresolvedCheckpoint(state: PreservationRecord | null): boolean {
    return state?.checkpoint?.phase === "capturing" || state?.checkpoint?.phase === "unknown";
  }

  snapshot(): SandboxPreservationState | null {
    const state = this.readState();
    return state
      ? sandboxPreservationSchema.parse({
          ...state,
          savedAtMs: state.receipt?.savedAtMs ?? state.savedAtMs,
          hasRecoveryPoint: !!state.receipt,
        })
      : null;
  }

  private current(state: PreservationRecord): boolean {
    const row = this.deps.sandbox.getSandbox();
    return (
      row?.modal_sandbox_id === state.generation.sandboxId &&
      row.created_at === state.generation.createdAt
    );
  }

  private publish(state: PreservationRecord): void {
    this.deps.store.write(state);
    this.deps.log?.info("sandbox.preservation", {
      event: "sandbox.preservation",
      phase: state.phase,
      provider: this.deps.provider.name,
      sandbox_id: state.generation.sandboxId,
      generation_created_at: state.generation.createdAt,
      operation_id: state.operationId,
      expires_at_ms: state.expiresAtMs,
    });
    this.deps.messenger.broadcast({
      type: "sandbox_preservation",
      preservation: sandboxPreservationSchema.parse({
        ...state,
        savedAtMs: state.receipt?.savedAtMs ?? state.savedAtMs,
        hasRecoveryPoint: !!state.receipt,
      }),
    });
  }

  /** Called before the provider can start a bridge for this generation. */
  beginGeneration(generation: SandboxGeneration): void {
    if (!generation.sandboxId) throw new Error("Missing sandbox generation");
    const previous = this.readState();
    if (this.unresolvedCheckpoint(previous) && !previous?.sourceRetired)
      throw new Error("An unresolved checkpoint still owns the source sandbox");
    this.publish({
      phase: "running",
      generation: { ...generation, sandboxId: generation.sandboxId },
      provider: this.deps.provider.name,
      providerObjectId: null,
      sourceRetired: previous?.phase === "saved",
      lifetimeKind: "unknown",
      expiresAtMs: null,
      drainAtMs: null,
      generationReady: false,
      receipt: previous?.receipt,
    });
  }

  /** Persist uncertainty before restore/resume can create or reactivate execution. */
  restoreStarting(generation: SandboxGeneration, providerObjectId?: string): void {
    const state = this.readState();
    if (!state || !this.current(state) || !this.matches(state, generation))
      throw new Error("Saved sandbox restore generation was superseded");
    this.publish({ ...state, sourceRetired: false, providerObjectId: providerObjectId ?? null });
  }

  async started(generation: SandboxGeneration, lifetime: SandboxLifetime): Promise<void> {
    const state = this.readState();
    if (
      !state ||
      !this.current(state) ||
      state.generation.createdAt !== generation.createdAt ||
      state.generation.sandboxId !== generation.sandboxId
    )
      return;
    const row = this.deps.sandbox.getSandbox();
    const settings = parsePersistedSandboxSettings(
      this.deps.session.getSession()?.sandbox_settings ?? null
    );
    const buffer = settings.finalSnapshotBufferMs ?? DEFAULT_FINAL_SNAPSHOT_BUFFER_MS;
    const expiresAtMs = lifetime.kind === "finite" ? lifetime.expiresAtMs : null;
    const next: PreservationRecord = {
      ...state,
      providerObjectId: row?.modal_object_id ?? null,
      sourceRetired: false,
      lifetimeKind: lifetime.kind,
      expiresAtMs,
      drainAtMs: expiresAtMs === null ? null : expiresAtMs - buffer,
    };
    this.publish(next);
    if (lifetime.kind === "unknown") {
      this.fail(
        next,
        "unknown",
        "Provider expiry could not be established; automatic dispatch is held."
      );
      return;
    }
    if (next.phase !== "running") return;
    this.bindGeneration(next);
    if (next.drainAtMs !== null) {
      if (this.now() >= next.drainAtMs) await this.request("sandbox_lifetime_expiring");
      else await this.deps.alarm.schedule(next.drainAtMs);
    }
    this.kickQueue();
  }

  runtimeReady(version?: 1): void {
    const state = this.readState();
    if (!state || !this.current(state)) return;
    const next = { ...state, runtimeReady: true, protocolVersion: version };
    this.publish(next);
    if (version !== 1) {
      this.fail(
        next,
        "failed",
        "This sandbox runtime does not support confirmed preservation. Upgrade the runtime before resuming work."
      );
      return;
    }
    this.bindGeneration(next);
  }

  private bindGeneration(state: PreservationRecord): void {
    const socket = this.deps.sockets.getSandboxSocket();
    if (socket && state.protocolVersion === 1) {
      this.deps.sockets.send(socket, { type: "sandbox_generation", generation: state.generation });
    }
  }

  generationReady(event: Extract<SandboxEvent, { type: "sandbox_generation_ready" }>): void {
    const state = this.readState();
    if (!state || !this.matches(state, event.generation) || !this.current(state)) return;
    this.publish({ ...state, generationReady: true });
    if (state.phase === "draining") this.kickAdvance();
    else this.kickQueue();
  }

  /** Synchronous admission gate; call again after every dispatch-path await. */
  mayDispatch(): boolean {
    const state = this.readState();
    if (!state) return true; // Legacy generations retain their existing policy until a new launch.
    if (state.phase === "saved") return true; // Existing queue drives restore, never prompt replay.
    if (state.phase !== "running" || !this.current(state)) return false;
    if (!this.providerMatches(state)) return false;
    // A provider-create failure with no connected runtime/receipt still uses
    // the existing fresh-spawn retry policy. Unknown preservation never does.
    if (
      !state.runtimeReady &&
      !state.receipt &&
      !state.providerObjectId &&
      this.deps.sandbox.getSandbox()?.status === "failed"
    )
      return true;
    if (state.drainAtMs !== null && this.now() >= state.drainAtMs) {
      this.deps.background.submit(() => this.request("sandbox_lifetime_expiring"), {
        name: "sandbox.preserve",
      });
      return false;
    }
    return state.lifetimeKind !== "unknown" && state.generationReady;
  }

  isHolding(): boolean {
    const phase = this.readState()?.phase;
    return phase !== undefined && phase !== "running" && phase !== "saved";
  }

  recoveryReceipt() {
    const state = this.readState();
    return state?.phase === "saved" ? state.receipt : undefined;
  }

  restoreFailed(error: string, generation?: SandboxGeneration): void {
    const state = this.readState();
    if (!state || !this.current(state) || (generation && !this.matches(state, generation))) return;
    if (state.receipt)
      this.fail(
        { ...state, sourceRetired: state.sourceRetired || state.phase === "saved" },
        "unknown",
        `Saved sandbox could not be restored: ${error}. No fresh sandbox was substituted.`
      );
  }

  /** Only an explicit authenticated user choice may leave a failed/unknown hold. */
  async recover(action: "retry" | "restore_saved"): Promise<void> {
    const state = this.readState();
    if (!state || (state.phase !== "failed" && state.phase !== "unknown") || !this.current(state))
      return;
    if (action === "retry") {
      // Unknown means provider I/O may still have run; never repeat that capture blindly.
      if (state.phase !== "failed")
        throw new Error(
          "An unknown provider result cannot be retried safely; restore a saved recovery point or start a separate session."
        );
      this.publish({ ...state, phase: "running", error: undefined });
      await this.request(state.reason ?? "preservation_retry");
      return;
    }
    if (!state.receipt || state.receipt.provider !== this.deps.provider.name)
      throw new Error("No saved recovery point for the configured provider is available.");
    const next: PreservationRecord = {
      ...state,
      phase: "retiring",
      reason: "restore_saved_state",
      error: undefined,
      operationId: crypto.randomUUID(),
      retireByMs: this.now() + RETIRE_MS,
    };
    this.publish(next);
    if (state.sourceRetired || (state.expiresAtMs !== null && this.now() >= state.expiresAtMs)) {
      // The hard provider deadline independently proves the old execution ended.
      this.finish(next);
    } else if (state.providerObjectId) await this.retire(next);
    else
      this.fail(
        next,
        "unknown",
        "The source provider handle is unknown; retirement cannot be verified."
      );
  }

  /** Capture-only serialization: a verified nondestructive checkpoint does not close work. */
  async checkpoint(reason: string): Promise<CheckpointOutcome> {
    const state = this.readState();
    const { provider } = this.deps;
    if (!state) return { kind: "skipped", reason: "legacy_generation_without_capture_owner" };
    if (provider.capabilities.snapshotStopsSandbox === true) {
      const requested = await this.request(reason);
      return {
        kind: "skipped",
        reason: requested ? "final_preservation_requested" : "preservation_held",
      };
    }
    if (!provider.takeSnapshot || provider.capabilities.snapshotStopsSandbox !== false)
      return { kind: "skipped", reason: "non_destructive_capture_not_supported" };
    if (state.phase !== "running" || !this.mayDispatch())
      return { kind: "skipped", reason: "preservation_held" };
    if (this.unresolvedCheckpoint(state)) return { kind: "skipped", reason: "capture_owned" };
    const row = this.deps.sandbox.getSandbox();
    const session = this.deps.session.getSession();
    if (
      !row?.modal_object_id ||
      !session ||
      (row.status !== "ready" && !(row.status === "stale" && reason === "heartbeat_timeout"))
    )
      return { kind: "skipped", reason: "sandbox_not_ready" };
    const startedAtMs = this.now();
    const operation: CheckpointOperation = {
      version: 1,
      operationId: crypto.randomUUID(),
      generation: state.generation,
      provider: provider.name,
      providerObjectId: row.modal_object_id,
      runtimeVersion: row.runtime_version,
      reason,
      startedAtMs,
      deadlineAtMs: Math.min(startedAtMs + CAPTURE_MS, state.expiresAtMs ?? Infinity),
      nonDestructive: true,
      phase: "capturing",
    };
    // The store, not the local promise, reserves the capture before the first await.
    this.deps.store.write({ ...state, checkpoint: operation });
    this.checkpointOperation = operation.operationId;
    let captureStarted = false;
    try {
      await this.deps.alarm.schedule(operation.deadlineAtMs);
      if (!this.ownsCheckpoint(operation)) return { kind: "skipped", reason: "superseded" };
      if (this.now() >= operation.deadlineAtMs) {
        this.deps.store.write({ ...this.readState()!, checkpoint: state.checkpoint });
        return { kind: "failed", reason: "checkpoint_deadline_elapsed_before_capture" };
      }
      captureStarted = true;
      const result = await this.bounded(operation.deadlineAtMs, (signal) =>
        provider.takeSnapshot!({
          providerObjectId: operation.providerObjectId,
          sessionId: session.session_name || session.id,
          reason,
          deadlineAtMs: operation.deadlineAtMs,
          signal,
        })
      );
      if (!this.ownsCheckpoint(operation)) return { kind: "skipped", reason: "superseded" };
      if (this.now() >= operation.deadlineAtMs) throw new PreservationDeadlineError();
      if (result.sourceStopped) {
        const current = this.readState()!;
        const error = "Provider stopped the source during a non-destructive checkpoint.";
        this.fail(
          { ...current, checkpoint: { ...operation, phase: "unknown", error } },
          "unknown",
          error
        );
        return { kind: "unknown", operationId: operation.operationId, reason: error };
      }
      if (!result.success || !result.imageId)
        throw new Error("Provider did not confirm a non-destructive checkpoint");
      const current = this.readState()!;
      const savedAtMs = this.now();
      this.deps.session.transaction(() => {
        if (
          !this.deps.sandbox.recordSandboxSnapshot(
            operation.generation.sandboxId,
            result.imageId!,
            operation.runtimeVersion
          )
        )
          throw new Error("Checkpoint generation was not recorded");
        this.deps.store.write({
          ...current,
          checkpoint: { ...operation, phase: "completed", imageId: result.imageId!, savedAtMs },
        });
      });
      // Publication failure cannot undo the committed outcome or turn it into unknown.
      this.deps.messenger.broadcast({ type: "snapshot_saved", imageId: result.imageId, reason });
      return { kind: "completed", operationId: operation.operationId, imageId: result.imageId };
    } catch {
      const current = this.readState();
      const completed = current?.checkpoint;
      if (completed?.operationId === operation.operationId && completed.phase === "completed")
        return {
          kind: "completed",
          operationId: operation.operationId,
          imageId: completed.imageId,
        };
      if (!this.ownsCheckpoint(operation)) return { kind: "skipped", reason: "superseded" };
      if (!captureStarted) {
        this.deps.store.write({ ...current!, checkpoint: state.checkpoint });
        return { kind: "failed", reason: "checkpoint_deadline_could_not_be_armed" };
      }
      const error = "Checkpoint result is unknown; capture will not be repeated automatically.";
      this.deps.store.write({ ...current!, checkpoint: { ...operation, phase: "unknown", error } });
      return { kind: "unknown", operationId: operation.operationId, reason: error };
    } finally {
      if (this.checkpointOperation === operation.operationId) this.checkpointOperation = null;
      if (this.readState()?.phase === "waiting_for_checkpoint") this.kickAdvance();
    }
  }

  private ownsCheckpoint(operation: CheckpointOperation): boolean {
    const state = this.readState();
    const row = this.deps.sandbox.getSandbox();
    return (
      !!state &&
      this.current(state) &&
      state.checkpoint?.operationId === operation.operationId &&
      state.checkpoint.phase === "capturing" &&
      state.generation.sandboxId === operation.generation.sandboxId &&
      state.generation.createdAt === operation.generation.createdAt &&
      row?.modal_object_id === operation.providerObjectId &&
      state.providerObjectId === operation.providerObjectId &&
      this.deps.provider.name === operation.provider
    );
  }

  /** Heartbeat recovery may retire its own checkpoint, never a final owner's source. */
  async retireHeartbeatCheckpoint(operationId: string): Promise<void> {
    const state = this.readState();
    const checkpoint = state?.checkpoint;
    if (
      !state ||
      state.phase !== "running" ||
      !this.current(state) ||
      checkpoint?.phase !== "completed" ||
      checkpoint.operationId !== operationId ||
      checkpoint.reason !== "heartbeat_timeout" ||
      checkpoint.provider !== this.deps.provider.name ||
      this.deps.sandbox.getSandbox()?.modal_object_id !== checkpoint.providerObjectId
    )
      return;
    const retiring: PreservationRecord = {
      ...state,
      phase: "retiring",
      operationId,
      reason: "heartbeat_timeout",
      retireByMs: Math.min(this.now() + RETIRE_MS, state.expiresAtMs ?? Infinity),
      receipt: {
        kind: "snapshot",
        artifactId: checkpoint.imageId,
        provider: checkpoint.provider,
        savedAtMs: checkpoint.savedAtMs,
        runtimeVersion: checkpoint.runtimeVersion,
      },
      savedAtMs: checkpoint.savedAtMs,
    };
    // Synchronous ownership acquisition precedes provider teardown. A final
    // request cannot enter while retirement is awaiting its provider result.
    this.publish(retiring);
    await this.retire(retiring);
  }

  async request(reason: string): Promise<boolean> {
    const state = this.readState();
    if (!state) {
      const row = this.deps.sandbox.getSandbox();
      if (!row?.modal_sandbox_id || !row.modal_object_id) return false;
      this.fail(
        {
          phase: "unknown",
          generation: { sandboxId: row.modal_sandbox_id, createdAt: row.created_at },
          provider: this.deps.provider.name,
          providerObjectId: row.modal_object_id,
          lifetimeKind: "unknown",
          expiresAtMs: null,
          drainAtMs: null,
          generationReady: false,
          reason,
        },
        "unknown",
        "Legacy sandbox has no capture owner; automatic retirement is held until recovery is verified."
      );
      return true;
    }
    if (!state || !this.current(state) || state.phase !== "running") return false;
    if (!this.providerMatches(state)) return true;
    const now = this.now();
    const waiting = this.unresolvedCheckpoint(state) || !!state.checkpointInFlight;
    const waitEnd = waiting ? Math.max(now, state.checkpoint?.deadlineAtMs ?? now) : now;
    const end = state.expiresAtMs ?? waitEnd + STOP_MS + CAPTURE_MS + RETIRE_MS + MARGIN_MS;
    const retireByMs = end - MARGIN_MS;
    const next: PreservationRecord = {
      ...state,
      phase: waiting ? "waiting_for_checkpoint" : "draining",
      reason,
      operationId: crypto.randomUUID(),
      ...(waiting
        ? {
            waitByMs: Math.min(waitEnd, retireByMs - RETIRE_MS - STOP_MS),
            stopByMs: undefined,
            captureByMs: undefined,
          }
        : this.preparationBudget(now, retireByMs)),
      retireByMs,
    };
    const failure = this.deps.session.transaction(() => {
      const message = this.deps.messages.getProcessingMessage();
      if (message) next.messageId = message.id;
      this.deps.store.write(next); // Fence before any asynchronous work or terminal publication.
      return message ? this.deps.failures.record(message.id, reason, now, "processing") : null;
    });
    this.publish(next);
    if (failure) this.deps.failures.deliver(failure);
    // Withdraw application access when final ownership is acquired. This does
    // not revoke provider-issued capabilities or stop external writers.
    this.deps.messenger.broadcast({ type: "sandbox_access_changed" });
    this.deps.messenger.broadcast({ type: "processing_status", isProcessing: false });
    this.deps.background.submit(() => this.deps.reconcileStatus(), {
      name: "sandbox.preservation_status",
    });
    await this.advance();
    return true;
  }

  prepared(event: Extract<SandboxEvent, { type: "preservation_prepared" }>): void {
    const state = this.readState();
    if (
      !state ||
      !this.current(state) ||
      !this.matches(state, event.generation) ||
      state.operationId !== event.operationId ||
      this.unresolvedCheckpoint(state) ||
      state.checkpointInFlight ||
      state.phase !== "draining"
    )
      return;
    if (!event.executionStopped || this.now() > state.stopByMs!) {
      this.fail(
        state,
        "failed",
        event.error ?? "Active execution did not stop before the preservation deadline."
      );
      return;
    }
    this.publish({ ...state, phase: "prepared" }); // Durable evidence before the critical-event ACK.
    this.kickAdvance();
  }

  /** Runs before generic watchdogs, and reasserts the absolute deadline on every alarm. */
  async handleAlarm(): Promise<boolean> {
    const state = this.readState();
    if (!state) return false;
    if (state.phase === "running") {
      if (
        state.checkpoint?.phase === "capturing" &&
        (this.checkpointOperation !== state.checkpoint.operationId ||
          this.now() >= state.checkpoint.deadlineAtMs)
      ) {
        this.deps.store.write({
          ...state,
          checkpoint: {
            ...state.checkpoint,
            phase: "unknown",
            error: "Checkpoint result was lost or exceeded its deadline.",
          },
        });
      } else if (state.checkpoint?.phase === "capturing") {
        await this.deps.alarm.schedule(state.checkpoint.deadlineAtMs);
      }
      if (state.drainAtMs !== null) {
        if (this.now() >= state.drainAtMs) await this.request("sandbox_lifetime_expiring");
        else await this.deps.alarm.schedule(state.drainAtMs);
      }
      return this.isHolding();
    }
    if (state.phase === "saved") return false;
    await this.advance();
    return true;
  }

  private async advance(): Promise<void> {
    let state = this.readState();
    if (!state || !this.current(state) || !state.operationId) return;
    if (!this.providerMatches(state)) return;
    if (state.phase === "waiting_for_checkpoint") {
      if (this.unresolvedCheckpoint(state) || state.checkpointInFlight) {
        if (
          state.checkpoint?.phase !== "capturing" ||
          this.checkpointOperation !== state.checkpoint.operationId ||
          this.now() >= state.waitByMs!
        ) {
          this.fail(
            state,
            "unknown",
            "An earlier checkpoint has an unknown result or exceeded the final wait deadline."
          );
          return;
        }
        await this.deps.alarm.schedule(state.waitByMs!);
        if (!this.owns(state)) return;
        state = this.readState()!;
        if (this.unresolvedCheckpoint(state)) return;
      }
      if (this.now() >= state.waitByMs!) {
        this.fail(
          state,
          "failed",
          "No preparation budget remains after waiting for the checkpoint."
        );
        return;
      }
      state = {
        ...state,
        phase: "draining",
        ...this.preparationBudget(this.now(), state.retireByMs!),
      };
      this.publish(state);
    }
    if (state.phase === "draining") {
      if (this.now() >= state.stopByMs!) {
        this.fail(
          state,
          "failed",
          "Could not confirm prompt/tool shutdown before the preservation deadline."
        );
        return;
      }
      await this.deps.alarm.schedule(state.stopByMs!);
      // Scheduling yields: a checkpoint or a newer final operation may finish
      // while it is pending. Never publish a pre-await copy over that result.
      if (!this.owns(state)) return;
      state = this.readState()!;
      if (this.unresolvedCheckpoint(state) || state.checkpointInFlight) {
        if (
          state.checkpoint?.phase !== "capturing" ||
          this.checkpointOperation !== state.checkpoint.operationId
        )
          this.fail(state, "unknown", "An earlier checkpoint has an unknown result.");
        return;
      }
      if (!state.generationReady || state.protocolVersion !== 1) return;
      const socket = this.deps.sockets.getSandboxSocket();
      if (socket)
        this.deps.sockets.send(socket, {
          type: "prepare_preservation",
          operationId: state.operationId,
          generation: state.generation,
          messageId: state.messageId,
          stopByMs: state.stopByMs!,
        });
      return;
    }
    if (state.phase === "capturing") {
      if (this.activeOperation !== state.operationId)
        this.fail(
          state,
          "unknown",
          "Preservation was interrupted; the provider result is unknown. No destructive retry was made."
        );
      return;
    }
    if (state.phase === "prepared") await this.capture(state);
    else if (state.phase === "retiring") await this.retire(state);
  }

  private async capture(state: PreservationRecord): Promise<void> {
    const { provider } = this.deps;
    if (!state.providerObjectId || this.now() >= state.captureByMs!) {
      this.fail(state, "failed", "No time or provider handle remains for a final snapshot.");
      return;
    }
    this.activeOperation = state.operationId!;
    const runtimeVersion = this.deps.sandbox.getSandbox()?.runtime_version ?? null;
    const capturing = { ...state, phase: "capturing" as const };
    this.publish(capturing);
    try {
      await this.deps.alarm.schedule(state.captureByMs!);
      if (!this.owns(capturing)) return;
      const retained =
        !!provider.capabilities.supportsPersistentResume &&
        !provider.capabilities.supportsSnapshots;
      const session = this.deps.session.getSession()!;
      const common = {
        providerObjectId: state.providerObjectId,
        sessionId: session.session_name || session.id,
        reason: state.reason!,
        deadlineAtMs: state.captureByMs!,
      };
      let artifactId = state.providerObjectId;
      let sourceStopped = retained;
      if (retained) {
        if (!provider.stopSandbox) throw new Error("Provider cannot preserve-stop this sandbox");
        const result = await this.bounded(state.captureByMs!, (signal) =>
          provider.stopSandbox!({ ...common, intent: "preserve", signal })
        );
        if (!result.success)
          throw new Error(result.error ?? "Provider did not confirm preservation");
      } else {
        if (!provider.takeSnapshot) throw new Error("Provider has no snapshot operation");
        const result = await this.bounded(state.captureByMs!, (signal) =>
          provider.takeSnapshot!({ ...common, signal })
        );
        if (!result.success || !result.imageId)
          throw new Error(result.error ?? "Provider did not return a ready snapshot");
        artifactId = result.imageId;
        sourceStopped = result.sourceStopped === true;
      }
      if (!this.owns(capturing)) return;
      if (this.now() >= state.captureByMs!) throw new PreservationDeadlineError();
      const receipt = {
        kind: retained ? ("retained" as const) : ("snapshot" as const),
        artifactId,
        provider: provider.name,
        savedAtMs: this.now(),
        runtimeVersion,
      };
      const retiring: PreservationRecord = {
        ...capturing,
        phase: "retiring",
        receipt,
        savedAtMs: receipt.savedAtMs,
      };
      this.publish(retiring); // Commit recovery locator BEFORE separately retiring the source.
      if (!retained)
        this.deps.sandbox.recordSandboxSnapshot(
          state.generation.sandboxId,
          artifactId,
          receipt.runtimeVersion
        );
      if (sourceStopped) this.finish(retiring);
      else await this.retire(retiring);
    } catch (error) {
      if (this.owns(capturing))
        this.fail(
          capturing,
          "unknown",
          error instanceof PreservationDeadlineError
            ? "Provider preservation deadline exceeded; result unknown."
            : "The provider did not confirm final preservation. The previous recovery point is unchanged."
        );
    } finally {
      this.activeOperation = null;
    }
  }

  private preparationBudget(now: number, retireByMs: number) {
    const stopByMs = Math.min(now + STOP_MS, retireByMs - RETIRE_MS);
    return { stopByMs, captureByMs: Math.min(stopByMs + CAPTURE_MS, retireByMs - RETIRE_MS) };
  }

  private async retire(state: PreservationRecord): Promise<void> {
    if (this.retiringOperation === state.operationId) return;
    if (!state.receipt || !state.providerObjectId) return;
    if (this.now() >= state.retireByMs!) {
      this.fail(state, "unknown", "Recovery point saved, but source retirement was not confirmed.");
      return;
    }
    this.retiringOperation = state.operationId!;
    try {
      if (!this.deps.provider.stopSandbox)
        throw new Error("Provider cannot confirm source retirement");
      const session = this.deps.session.getSession()!;
      const deadlineAtMs = Math.min(state.retireByMs!, this.now() + RETIRE_MS);
      await this.deps.alarm.schedule(deadlineAtMs);
      const result = await this.bounded(deadlineAtMs, (signal) =>
        this.deps.provider.stopSandbox!({
          providerObjectId: state.providerObjectId!,
          sessionId: session.session_name || session.id,
          reason: state.reason!,
          intent: state.receipt!.kind === "snapshot" ? "destroy" : "preserve",
          deadlineAtMs,
          signal,
        })
      );
      if (!result.success) throw new Error(result.error ?? "Source retirement failed");
      if (this.owns(state)) this.finish(state);
    } catch {
      if (this.owns(state))
        this.fail(
          state,
          "unknown",
          "A recovery point is saved, but source retirement could not be confirmed."
        );
    } finally {
      this.retiringOperation = null;
    }
  }

  private finish(state: PreservationRecord): void {
    // A restore preflight may fail before acquiring any new source. Its durable
    // retirement proof applies to the generation even though its handle is null.
    const expectedObject = state.sourceRetired
      ? (this.deps.sandbox.getSandbox()?.modal_object_id ?? null)
      : state.providerObjectId;
    if (!this.owns(state) || !this.deps.completePreservation(state.generation, expectedObject))
      return;
    this.publish({ ...state, phase: "saved", sourceRetired: true, checkpointInFlight: undefined });
    this.deps.messenger.broadcast({ type: "sandbox_status", status: "stopped" });
    this.kickQueue();
  }

  private fail(state: PreservationRecord, phase: "failed" | "unknown", error: string): void {
    this.publish({ ...state, phase, error });
    this.deps.messenger.broadcast({
      type: "sandbox_warning",
      message: `Sandbox preservation ${phase}: ${error}`,
    });
  }

  private owns(state: PreservationRecord): boolean {
    const current = this.readState();
    return (
      this.current(state) &&
      (state.sourceRetired ||
        this.deps.sandbox.getSandbox()?.modal_object_id === state.providerObjectId) &&
      (!state.provider || state.provider === this.deps.provider.name) &&
      current !== null &&
      current.operationId === state.operationId &&
      current.phase === state.phase
    );
  }

  private matches(state: PreservationRecord, generation: SandboxGeneration): boolean {
    return (
      state.generation.sandboxId === generation.sandboxId &&
      state.generation.createdAt === generation.createdAt
    );
  }

  private providerMatches(state: PreservationRecord): boolean {
    if (!state.provider || state.provider === this.deps.provider.name) return true;
    if (state.phase !== "unknown")
      this.fail(
        state,
        "unknown",
        "The sandbox provider changed; its existing source cannot be preserved through a different provider."
      );
    return false;
  }

  private kickQueue(): void {
    this.deps.background.submit(() => this.deps.processQueue(), { name: "message_queue.process" });
  }

  private kickAdvance(): void {
    this.deps.background.submit(() => this.advance(), { name: "sandbox.preservation_advance" });
  }

  private async bounded<T>(
    deadline: number,
    operation: (signal: AbortSignal) => Promise<T>
  ): Promise<T> {
    if (this.now() >= deadline) throw new PreservationDeadlineError();
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(
        () => {
          controller.abort();
          reject(
            new PreservationDeadlineError("Provider preservation deadline exceeded; result unknown")
          );
        },
        Math.max(0, deadline - this.now())
      );
    });
    try {
      return await Promise.race([operation(controller.signal), timeout]);
    } finally {
      clearTimeout(timer!);
    }
  }
}
