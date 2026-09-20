import { describe, expect, it } from "vitest";
import type { SessionDO } from "../../src/cloudflare/durable-object";
import {
  collectMessages,
  initNamedSession,
  openSandboxWs,
  queryDO,
  seedMessage,
  seedSandboxAuth,
} from "./helpers";
import { componentsOf, runInSessionDO } from "./session-do-access";

const AUTH_TOKEN = "preservation-integration-token";
const SANDBOX_ID = "preservation-sandbox";

interface SandboxGeneration {
  sandboxId: string;
  createdAt: number;
}

async function seedPreservation(
  stub: DurableObjectStub,
  overrides: Record<string, unknown> = {}
): Promise<SandboxGeneration> {
  const [sandbox] = await queryDO<{ created_at: number }>(stub, "SELECT created_at FROM sandbox");
  const generation = { sandboxId: SANDBOX_ID, createdAt: sandbox.created_at };
  const now = Date.now();
  const state = {
    phase: "running",
    generation,
    providerObjectId: null,
    lifetimeKind: "finite",
    expiresAtMs: now + 30 * 60_000,
    drainAtMs: now + 20 * 60_000,
    generationReady: false,
    ...overrides,
  };
  await runInSessionDO(stub, (_instance: SessionDO, durableState) => {
    durableState.storage.sql.exec(
      `INSERT INTO sandbox_preservation (singleton, state) VALUES (1, ?)
       ON CONFLICT(singleton) DO UPDATE SET state = excluded.state`,
      JSON.stringify(state)
    );
  });
  return generation;
}

async function readPreservation(stub: DurableObjectStub): Promise<Record<string, unknown>> {
  const [row] = await queryDO<{ state: string }>(
    stub,
    "SELECT state FROM sandbox_preservation WHERE singleton = 1"
  );
  return JSON.parse(row.state) as Record<string, unknown>;
}

describe("sandbox preservation wiring", () => {
  it.each(["lifetime", "stop_timeout"])(
    "allows checkpoint-time access and commands, but blocks both after %s preservation",
    async (trigger) => {
      const name = `checkpoint-admission-${Date.now()}`;
      const { stub } = await initNamedSession(name);
      await seedSandboxAuth(stub, { authToken: AUTH_TOKEN, sandboxId: SANDBOX_ID });
      const generation = await seedPreservation(stub, {
        provider: "modal",
        providerObjectId: "checkpoint-source",
        generationReady: true,
        runtimeReady: true,
        protocolVersion: 1,
      });
      const now = Date.now();
      await runInSessionDO(stub, (_instance, state) => {
        state.storage.sql.exec(
          "UPDATE sandbox SET modal_object_id = ?, last_heartbeat = ?, last_activity = ?",
          "checkpoint-source",
          now,
          now
        );
        const row = state.storage.sql
          .exec<{ state: string }>("SELECT state FROM sandbox_preservation")
          .one();
        const record = JSON.parse(row.state);
        record.checkpoint = {
          version: 1,
          operationId: "checkpoint-op",
          generation,
          provider: "modal",
          providerObjectId: "checkpoint-source",
          runtimeVersion: "v71-test",
          reason: "execution_complete",
          startedAtMs: now,
          deadlineAtMs: now + 300_000,
          nonDestructive: true,
          phase: "capturing",
        };
        state.storage.sql.exec("UPDATE sandbox_preservation SET state = ?", JSON.stringify(record));
      });
      const { ws } = await openSandboxWs(name, { authToken: AUTH_TOKEN, sandboxId: SANDBOX_ID });
      expect(ws).not.toBeNull();
      ws!.accept();
      expect((await stub.fetch("http://internal/internal/sandbox-access")).status).toBe(200);
      expect(
        await runInSessionDO(
          stub,
          (instance) => componentsOf(instance).wsManager.getSandboxCommandTarget().kind
        )
      ).toBe("dispatch");

      // Real alarm wiring recognizes the lost local capture promise after restart.
      await runInSessionDO(stub, (instance) => instance.alarm());
      expect(await readPreservation(stub)).toMatchObject({
        phase: "running",
        checkpoint: { phase: "unknown" },
      });
      expect((await stub.fetch("http://internal/internal/sandbox-access")).status).toBe(200);
      if (trigger === "stop_timeout") {
        await runInSessionDO(stub, (instance) =>
          componentsOf(instance).lifecycleManager.terminateUnresponsiveSandbox(
            "stop_confirmation_timeout"
          )
        );
      } else {
        await runInSessionDO(stub, (_instance, state) => {
          const row = state.storage.sql
            .exec<{ state: string }>("SELECT state FROM sandbox_preservation")
            .one();
          state.storage.sql.exec(
            "UPDATE sandbox_preservation SET state = ?",
            JSON.stringify({ ...JSON.parse(row.state), drainAtMs: Date.now() - 1 })
          );
        });
        await runInSessionDO(stub, (instance) => instance.alarm());
      }
      expect(await readPreservation(stub)).toMatchObject({
        phase: "unknown",
        checkpoint: { phase: "unknown" },
      });
      expect((await stub.fetch("http://internal/internal/sandbox-access")).status).toBe(409);
      expect(
        await runInSessionDO(
          stub,
          (instance) => componentsOf(instance).wsManager.getSandboxCommandTarget().kind
        )
      ).toBe("unavailable");
      expect(await queryDO(stub, "SELECT status FROM sandbox")).toEqual([{ status: "ready" }]);
      ws!.close();
    }
  );

  it("holds a persisted legacy snapshotting row instead of inferring readiness", async () => {
    const name = `checkpoint-legacy-${Date.now()}`;
    const { stub } = await initNamedSession(name);
    await seedSandboxAuth(stub, {
      authToken: AUTH_TOKEN,
      sandboxId: SANDBOX_ID,
      status: "snapshotting",
    });
    await runInSessionDO(stub, (instance) => instance.alarm());
    expect(await readPreservation(stub)).toMatchObject({
      phase: "unknown",
      reason: "legacy_checkpoint",
    });
    expect((await stub.fetch("http://internal/internal/sandbox-access")).status).toBe(409);
    expect(await queryDO(stub, "SELECT status FROM sandbox")).toEqual([{ status: "snapshotting" }]);
  });

  it("holds queued work until a versioned runtime acknowledges its sandbox generation", async () => {
    const name = `preservation-generation-${Date.now()}`;
    const { stub } = await initNamedSession(name);
    await seedSandboxAuth(stub, { authToken: AUTH_TOKEN, sandboxId: SANDBOX_ID });
    const generation = await seedPreservation(stub);
    const [{ id: authorId }] = await queryDO<{ id: string }>(
      stub,
      "SELECT id FROM participants LIMIT 1"
    );
    await seedMessage(stub, {
      id: "preservation-pending",
      authorId,
      content: "Run only after generation acknowledgement",
      source: "web",
      status: "pending",
      createdAt: Date.now(),
    });

    const { ws } = await openSandboxWs(name, {
      authToken: AUTH_TOKEN,
      sandboxId: SANDBOX_ID,
    });
    expect(ws).not.toBeNull();
    ws!.accept();
    const commands = collectMessages(ws!, {
      until: (message) => message.type === "sandbox_generation",
      timeoutMs: 2_000,
    });
    ws!.send(
      JSON.stringify({
        type: "ready",
        sandboxId: SANDBOX_ID,
        timestamp: Date.now() / 1000,
        preservationProtocolVersion: 1,
      })
    );

    expect(await commands).toContainEqual({ type: "sandbox_generation", generation });
    expect(
      await queryDO<{ status: string }>(
        stub,
        "SELECT status FROM messages WHERE id = ?",
        "preservation-pending"
      )
    ).toEqual([{ status: "pending" }]);

    const delivered = collectMessages(ws!, {
      until: (message) => message.type === "prompt",
      timeoutMs: 2_000,
    });
    ws!.send(
      JSON.stringify({
        type: "sandbox_generation_ready",
        generation,
        sandboxId: SANDBOX_ID,
        timestamp: Date.now() / 1000,
        ackId: "generation-ready-ack",
      })
    );
    const messages = await delivered;
    expect(messages).toContainEqual({ type: "ack", ackId: "generation-ready-ack" });
    expect(messages).toContainEqual(
      expect.objectContaining({ type: "prompt", messageId: "preservation-pending" })
    );
    ws!.close();
  });

  it("drains once, holds pending work, and acknowledges only matching preparation state", async () => {
    const name = `preservation-drain-${Date.now()}`;
    const { stub } = await initNamedSession(name);
    await seedSandboxAuth(stub, { authToken: AUTH_TOKEN, sandboxId: SANDBOX_ID });
    const generation = await seedPreservation(stub, {
      drainAtMs: Date.now() - 1,
      generationReady: true,
      runtimeReady: true,
      protocolVersion: 1,
    });
    const [{ id: authorId }] = await queryDO<{ id: string }>(
      stub,
      "SELECT id FROM participants LIMIT 1"
    );
    await seedMessage(stub, {
      id: "preservation-processing",
      authorId,
      content: "Stop before preservation",
      source: "web",
      status: "processing",
      createdAt: Date.now() - 2_000,
      startedAt: Date.now() - 1_000,
    });
    await seedMessage(stub, {
      id: "preservation-held",
      authorId,
      content: "Remain pending",
      source: "web",
      status: "pending",
      createdAt: Date.now(),
    });

    const { ws } = await openSandboxWs(name, {
      authToken: AUTH_TOKEN,
      sandboxId: SANDBOX_ID,
    });
    expect(ws).not.toBeNull();
    ws!.accept();
    const preparation = collectMessages(ws!, {
      until: (message) => message.type === "prepare_preservation",
      timeoutMs: 2_000,
    });
    await runInSessionDO(stub, (instance: SessionDO) => instance.alarm());
    const prepare = (await preparation).find((message) => message.type === "prepare_preservation");
    expect(prepare).toMatchObject({
      generation,
      messageId: "preservation-processing",
      operationId: expect.any(String),
    });
    expect(
      await queryDO<{ id: string; status: string }>(
        stub,
        "SELECT id, status FROM messages WHERE id IN (?, ?) ORDER BY id",
        "preservation-processing",
        "preservation-held"
      )
    ).toEqual([
      { id: "preservation-held", status: "pending" },
      { id: "preservation-processing", status: "failed" },
    ]);
    await runInSessionDO(stub, (instance: SessionDO) => instance.alarm());
    expect(
      await queryDO<{ count: number }>(
        stub,
        "SELECT COUNT(*) AS count FROM events WHERE type = 'execution_complete' AND message_id = ?",
        "preservation-processing"
      )
    ).toEqual([{ count: 1 }]);

    const staleAck = collectMessages(ws!, {
      until: (message) => message.type === "ack" && message.ackId === "stale-prepared",
      timeoutMs: 2_000,
    });
    ws!.send(
      JSON.stringify({
        type: "preservation_prepared",
        operationId: "stale-operation",
        generation,
        executionStopped: true,
        sandboxId: SANDBOX_ID,
        timestamp: Date.now() / 1000,
        ackId: "stale-prepared",
      })
    );
    expect(await staleAck).toContainEqual({ type: "ack", ackId: "stale-prepared" });
    expect(await readPreservation(stub)).toMatchObject({ phase: "draining" });

    const matchingAck = collectMessages(ws!, {
      until: (message) => message.type === "ack" && message.ackId === "matching-prepared",
      timeoutMs: 2_000,
    });
    ws!.send(
      JSON.stringify({
        type: "preservation_prepared",
        operationId: prepare!.operationId,
        generation,
        executionStopped: true,
        sandboxId: SANDBOX_ID,
        timestamp: Date.now() / 1000,
        ackId: "matching-prepared",
      })
    );
    expect(await matchingAck).toContainEqual({ type: "ack", ackId: "matching-prepared" });
    expect(await readPreservation(stub)).toMatchObject({ phase: "failed" });
    ws!.close();
  });
});
