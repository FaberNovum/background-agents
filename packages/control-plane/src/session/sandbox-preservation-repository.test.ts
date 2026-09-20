import { DatabaseSync } from "node:sqlite";
import { describe, expect, it } from "vitest";
import { createNodeSqlStorage } from "../node/sqlite-storage";
import { initSchema } from "./schema";
import {
  SandboxPreservationRepository,
  type PreservationRecord,
} from "./sandbox-preservation-repository";
import { SessionStorageIntegrityError } from "./types";

function record(overrides: Partial<PreservationRecord> = {}): PreservationRecord {
  return {
    phase: "running",
    generation: { sandboxId: "sandbox-1", createdAt: 1_000 },
    providerObjectId: "provider-1",
    lifetimeKind: "finite",
    expiresAtMs: 20_000,
    drainAtMs: 10_000,
    generationReady: false,
    ...overrides,
  };
}

function repository() {
  const db = new DatabaseSync(":memory:");
  const { sql } = createNodeSqlStorage(db);
  initSchema(sql);
  return { db, sql, repository: new SandboxPreservationRepository(sql) };
}

describe("SandboxPreservationRepository", () => {
  const checkpoint = {
    version: 1 as const,
    operationId: "capture-1",
    generation: { sandboxId: "sandbox-1", createdAt: 1_000 },
    provider: "modal",
    providerObjectId: "provider-1",
    runtimeVersion: "v71-test",
    reason: "execution_complete",
    startedAtMs: 2_000,
    deadlineAtMs: 9_000,
    nonDestructive: true as const,
    phase: "capturing" as const,
  };

  it("round-trips capture ownership, uncertainty and completion without another journal", () => {
    const f = repository();
    for (const operation of [
      checkpoint,
      { ...checkpoint, phase: "unknown" as const, error: "deadline" },
      { ...checkpoint, phase: "completed" as const, imageId: "image", savedAtMs: 8_000 },
    ]) {
      const state = record({ provider: "modal", checkpoint: operation });
      f.repository.write(state);
      expect(f.repository.read()).toEqual(state);
    }
    f.db.close();
  });

  it.each([
    { version: 2 },
    { operationId: "" },
    { generation: { sandboxId: "other", createdAt: 1_000 } },
    { providerObjectId: "other" },
    { provider: "other" },
    { nonDestructive: false },
    { deadlineAtMs: 1_000 },
    { phase: "unknown" },
    { phase: "completed" },
  ])("rejects malformed checkpoint metadata %j without discarding its ownership", (bad) => {
    const f = repository();
    f.sql.exec(
      "INSERT INTO sandbox_preservation (singleton, state) VALUES (1, ?)",
      JSON.stringify(record({ provider: "modal", checkpoint: { ...checkpoint, ...bad } as never }))
    );
    expect(() => f.repository.read()).toThrow(SessionStorageIntegrityError);
    f.db.close();
  });
  it("distinguishes missing state from a stored running generation", () => {
    const fixture = repository();
    expect(fixture.repository.read()).toBeNull();

    fixture.repository.write(record());
    expect(fixture.repository.read()).toEqual(record());
    fixture.db.close();
  });

  it("atomically replaces the singleton while preserving a verified receipt", () => {
    const fixture = repository();
    fixture.repository.write(record());
    const saved = record({
      phase: "saved",
      sourceRetired: true,
      generationReady: true,
      operationId: "operation-1",
      receipt: {
        kind: "snapshot",
        artifactId: "image-1",
        provider: "modal",
        savedAtMs: 15_000,
        runtimeVersion: "runtime-1",
      },
      savedAtMs: 15_000,
    });

    fixture.repository.write(saved);

    expect(fixture.repository.read()).toEqual(saved);
    expect(
      fixture.sql.exec("SELECT COUNT(*) AS count FROM sandbox_preservation").toArray()
    ).toEqual([{ count: 1 }]);
    fixture.db.close();
  });

  it("fails closed on malformed persisted state", () => {
    const fixture = repository();
    fixture.sql.exec(
      "INSERT INTO sandbox_preservation (singleton, state) VALUES (1, ?)",
      JSON.stringify({ phase: "saved", generation: { sandboxId: "sandbox-1" } })
    );

    expect(() => fixture.repository.read()).toThrow(SessionStorageIntegrityError);
    fixture.db.close();
  });

  it("rejects a structurally complete saved phase without a verified receipt", () => {
    const fixture = repository();
    fixture.sql.exec(
      "INSERT INTO sandbox_preservation (singleton, state) VALUES (1, ?)",
      JSON.stringify({
        ...record(),
        phase: "saved",
        provider: "modal",
        generationReady: true,
        operationId: "operation-1",
        savedAtMs: 15_000,
      })
    );

    expect(() => fixture.repository.read()).toThrow(SessionStorageIntegrityError);
    fixture.db.close();
  });

  it("rejects invalid writes before replacing the last valid record", () => {
    const fixture = repository();
    fixture.repository.write(record());

    expect(() =>
      fixture.repository.write({ ...record(), generation: { sandboxId: "", createdAt: 1_000 } })
    ).toThrow();
    expect(fixture.repository.read()).toEqual(record());
    fixture.db.close();
  });
});
