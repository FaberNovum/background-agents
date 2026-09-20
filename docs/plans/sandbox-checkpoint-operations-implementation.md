# C2: checkpoint operations without lifecycle overloading

## Scope and dependencies

C2 builds on C1's lifecycle ownership ports and the final-preservation stack (runtime,
control-plane, and web recovery UI). It reuses `SandboxPreservation` and its validated singleton
`sandbox_preservation` record. There is no second coordinator, operation journal, SQL table, or
independently writable admission flag.

The local integration base includes C1's reviewed ports and the preservation runtime fixes through
`b1f9f012f`. These dependencies must land before deploying C2.

## Model

```mermaid
flowchart TD
    Ready[Ready sandbox] --> Reserve[Persist checkpoint identity and deadline]
    Reserve --> Capture[Verified nondestructive provider capture]
    Capture --> Completed[Commit artifact and completed operation atomically]
    Capture --> Unknown[Unknown result: retain capture ownership]
    Ready --> Final[Final preservation: close ordinary admission]
    Reserve -. final requested .-> Final
    Final --> Wait[Account for preceding capture]
    Completed --> Wait
    Wait --> Prepare[Confirm managed execution stopped]
    Prepare --> FinalCapture[New final capture, not the preceding checkpoint]
    FinalCapture --> Receipt[Persist final recovery receipt]
    Receipt --> Retire[Confirm source retirement]
    Retire --> Saved[Lifecycle owner commits stopped; publish saved]
    Unknown -. final requested .-> Hold[Visible unknown hold; no automatic retry]
```

The sandbox remains `ready` throughout an ordinary checkpoint. A checkpoint completion does not
restore a remembered status. A concurrent cancellation, failure, or replacement is never rewritten
to `ready`.

The nested checkpoint record contains a version, operation ID, generation (`sandboxId` and
`createdAt`), provider and object handle, captured runtime version, reason, absolute deadline,
nondestructive guarantee, and capture phase. Completion carries its artifact and timestamp;
uncertainty carries a safe error. Publication validates operation, generation, phase, and source
identity.

## Behavior matrix

| Situation                                               | Ordinary work and access                                        | New capture / replacement                             |
| ------------------------------------------------------- | --------------------------------------------------------------- | ----------------------------------------------------- |
| Ready, no capture                                       | Normal policy                                                   | Allowed                                               |
| Nondestructive checkpoint in flight                     | Normal policy; interactive access stays available               | Serialized behind the checkpoint                      |
| Nondestructive checkpoint result unknown                | Normal policy while the source remains ready                    | Held; a timeout is not proof remote capture ended     |
| Final preservation draining/prepared/capturing/retiring | Held, including access after async decryption and diff commands | Final owner only                                      |
| Final preservation failed/unknown                       | Held                                                            | Explicit supported recovery only                      |
| Verified saved final receipt                            | Existing restore admission policy                               | Restore without silently substituting a fresh sandbox |

Normal prompt, push, and diff commands resolve the same admission-aware socket target. Lifecycle
preparation, stop confirmation, heartbeat, and runtime facts retain their separate raw socket path.
Access checks admission both before and after secret decryption.

Idle expiry can begin final preservation during an ordinary checkpoint. It closes new work
immediately, waits for capture ownership, and requires a fresh matching preparation and final
capture. An unresolved checkpoint becomes a visible unknown hold instead of authorizing another
provider capture or source destruction. Unresponsive-execution recovery likewise closes admission
before its message stop hold can be cleared.

Heartbeat-stale recovery still permits a nondestructive capture of the existing source under the
same operation owner. Destruction follows only a completed capture; skipped, failed, or unknown
results enter the preservation hold.

## Outcomes and provider semantics

`CheckpointOutcome` explicitly distinguishes `completed`, `skipped`, `failed`, and `unknown`.
Failure before provider invocation can release the reservation. Once invocation may have occurred,
timeout, thrown errors, and unconfirmed results retain unknown ownership. Late completion cannot
publish after ownership or generation changes. A publication error cannot undo an already committed
receipt.

Ordinary capture requires the provider to explicitly declare `snapshotStopsSandbox: false` (Modal
and OpenComputer adapters). Vercel's destructive snapshot is routed through final preparation and
retirement. Providers without an explicit nondestructive guarantee skip ordinary capture; their
final-preservation protocol remains authoritative. A provider that reports stopping the source
despite its nondestructive declaration closes admission.

These are adapter contracts, not evidence of live-provider canary results.

## Legacy and rollout behavior

- No production path writes `snapshotting`; the status remains readable for compatibility. Legacy
  `snapshotting` rows or `checkpointInFlight: true` are converted to an unknown hold, never inferred
  ready or saved.
- A legacy live generation without a preservation record may continue ordinary work, but skips
  unowned checkpoint calls. When retirement is requested it enters an explicit unknown hold instead
  of destroying a source whose final capture was skipped. Without verified recovery evidence, use a
  separate session or operator recovery; do not clear the hold automatically.
- Malformed, unsupported-version, or inconsistent operation metadata fails closed in the repository
  decoder. Missing and malformed state are distinct.
- Keep the preservation runtime rebuild floor at 71 and the independent snapshot compatibility floor
  unchanged (62). Upgrade compatible runtime images before enabling this control-plane behavior.
- Before cutover, inventory legacy in-flight snapshots, active provider captures, legacy live
  generations, and unknown outcomes. Complete verified live captures or retain explicit holds; do
  not translate uncertainty into readiness.
- Use a controlled cutover. Old preservation readers strip unknown JSON fields; mixed-version
  writers can erase checkpoint ownership. Do not roll back to an operation-unaware binary while
  active/unknown ownership or admission holds exist. A rollback binary must retain this decoder and
  admission contract until those records are safely resolved.
- Provider canaries remain a release gate: Modal/OpenComputer concurrent work and capture; Vercel
  stop-on-snapshot; Daytona/E2B retained-state preservation; idle drain during checkpoint;
  interrupted capture; source retirement and restore. This implementation does not deploy or run
  paid live-provider operations.

## Verification

Focused tests cover durable decoder round trips and rejection, concurrent ordinary work, duplicate
capture, runtime provenance, cancellation and generation/object/ operation replacement, absolute
deadlines, restart, late completion, alarm await races, checkpoint-versus-final separation,
capability mismatch, and legacy holds. Real Workerd tests verify the composed command/access gates
and persisted restart and legacy behavior. Manager tests keep lifecycle transitions behind its
ports.

Full unit and Workerd suites, all control-plane typecheck configurations, Worker and Node builds,
lint and actual ESLint boundary tests are required before handoff. Live-provider validation is
separate from these deterministic checks.

### Local validation results

- Control-plane unit: **4,912 passed** across 307 files.
- Full Workerd integration: **1,311 passed, one skipped** across 110 files. Additional focused
  reruns cover the final checkpoint/stop-timeout wiring (five preservation tests).
- Shared package: **921 passed** across 57 files.
- Preservation runtime (bridge, Claude and OpenCode harness): **21 passed**.
- Modal snapshot-deadline tests: **6 passed**.
- Control-plane Worker, Node, unit-test and integration typechecks passed.
- Worker and Node builds, control-plane lint, formatting, and both actual ESLint-boundary tests
  passed.
- Independent architecture/race review: **Approve** after regression probes and fixes for stale
  alarm reads, absolute deadlines, legacy retirement, and stop-confirmation recovery admission;
  final small-delta review also approved.

The simplicity pass kept the outcome type in the existing lifecycle ports and removed the old
checkpoint boolean/generation pair and prior-status restoration. No generic operation framework or
additional coordinator was introduced.
