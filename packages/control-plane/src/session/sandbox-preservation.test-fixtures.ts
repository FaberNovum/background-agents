import { vi } from "vitest";
import type { SandboxEvent } from "@open-inspect/shared/types/sandbox-events";
import type { SandboxProvider } from "../sandbox/provider";
import { SandboxPreservation } from "./sandbox-preservation";
import type { PreservationRecord, PreservationStore } from "./sandbox-preservation-repository";
import type { SandboxRow, SessionRow, MessageRow } from "./types";

type PreservationDeps = ConstructorParameters<typeof SandboxPreservation>[0];
export const GENERATION = { sandboxId: "sandbox-1", createdAt: 1_000 };

export class MemoryStore implements PreservationStore {
  value: PreservationRecord | null = null;
  read() {
    return this.value;
  }
  write(record: PreservationRecord) {
    this.value = structuredClone(record);
  }
}

export function provider(overrides: Partial<SandboxProvider> = {}): SandboxProvider {
  return {
    name: "modal",
    createSandbox: vi.fn(async () => {
      throw new Error("Unexpected sandbox creation");
    }),
    capabilities: {
      supportsSandboxTimeout: true,
      supportsSnapshots: true,
      snapshotStopsSandbox: false,
      supportsRestore: true,
      supportsPersistentResume: false,
      supportsExplicitStop: true,
    },
    ...overrides,
  };
}

export function fixture(providerValue = provider()) {
  let now = 100_000;
  const store = new MemoryStore();
  const calls: string[] = [];
  const backgroundTasks: Array<() => Promise<unknown>> = [];
  const socket = { readyState: 1, send: vi.fn(), close: vi.fn() };
  const sandboxRow: SandboxRow = {
    id: "row-1",
    modal_sandbox_id: GENERATION.sandboxId,
    modal_object_id: "provider-object-1" as string | null,
    created_at: GENERATION.createdAt,
    runtime_version: "runtime-1",
    status: "ready",
    snapshot_id: null,
    snapshot_image_id: null,
    snapshot_runtime_version: null,
    auth_token: null,
    auth_token_hash: null,
    git_sync_status: "completed",
    last_heartbeat: null,
    last_activity: null,
    last_spawn_error: null,
    last_spawn_error_at: null,
    code_server_url: null,
    code_server_password: null,
    vnc_url: null,
    vnc_password: null,
    tunnel_urls: null,
    ttyd_url: null,
    ttyd_token: null,
    active_socket_id: null,
    boot_phase: null,
    boot_seq: null,
    fenced: 0,
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
      getSession: vi.fn(
        (): SessionRow => ({
          id: "session-1",
          session_name: "external-session-1",
          sandbox_settings: JSON.stringify({ finalSnapshotBufferMs: 600_000 }),
          title: null,
          repo_owner: "acme",
          repo_name: "repo",
          repo_id: null,
          base_branch: "main",
          branch_name: null,
          base_sha: null,
          current_sha: null,
          agent_session_id: null,
          harness: "opencode",
          model: "test-model",
          reasoning_effort: null,
          status: "active",
          status_revision: 0,
          parent_session_id: null,
          spawn_source: "user",
          spawn_depth: 0,
          code_server_enabled: 0,
          vnc_enabled: 0,
          total_cost: 0,
          max_cost_usd: null,
          budget_exhausted: 0,
          environment_id: null,
          created_at: 1_000,
          updated_at: 1_000,
        })
      ),
      transaction: <T>(fn: () => T): T => fn(),
    },
    messages: {
      getProcessingMessage: vi.fn<() => MessageRow | null>(() => null),
    },
    failures: {
      record: vi.fn<PreservationDeps["failures"]["record"]>(),
      deliver: vi.fn<PreservationDeps["failures"]["deliver"]>(),
    },
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
      submit: vi.fn<PreservationDeps["background"]["submit"]>((task) => {
        backgroundTasks.push(task);
      }),
    },
    processQueue: vi.fn(async () => undefined),
    reconcileStatus: vi.fn(async () => undefined),
    completePreservation: vi.fn(() => {
      calls.push("sandbox-stopped", "access-retired");
      return true;
    }),
    now: () => now,
  } satisfies PreservationDeps;
  const preservation = new SandboxPreservation(deps);
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

export async function readyFinite(f: ReturnType<typeof fixture>, expiresAtMs = 1_300_000) {
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

export async function readyWithoutDeadline(f: ReturnType<typeof fixture>) {
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

export function preparedEvent(
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
