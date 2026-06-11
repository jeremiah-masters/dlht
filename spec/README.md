# DLHT Quint Spec — Verification Methodology

This directory formally models the DLHT lock-free hash table and verifies it with
[Quint](https://github.com/informalsystems/quint) + Apalache. Run everything with
`./verify.sh` (see [Toolchain](#toolchain) for the pinned versions).

## What is modeled

`protocol.qnt` models the DLHT operations — Get, Insert, Put, Delete, and
LoadOrCompute (with the `Shadow` slot state) — against a **single fixed-size
index**, one atomic action per protocol step (PC-indexed state machines: Get
G1–G2, Insert I1–I6, Put P1–P7, Delete D1–D8, LoadOrCompute LC1–LC9).

Key abstractions:
- A bin is a flat array of `slotsPerBin` slots; link buckets / the link arena /
  LinkMeta are abstracted away.
- `slotKey: Option[KEY]` models the slot's hash field; `slotVal: Option[VAL]`
  the entry pointer (`None` = nil); `slotTag: Option[int]` + a global `nextTag`
  model entry-pointer **identity**, so Put/Delete's `(hash, ptr)` DWCAS compare
  is modeled exactly.
- The `Some(anyVal)` a Shadow slot carries through LoadOrCompute's fn window is
  **spec-only ghost state** (it lets the slot bear a tag); the Go contract is
  the **ownership form**, not an instant invariant: the entry pointer is nil at
  Shadow entry (by construction: reserve requires Invalid, and Invalid ⟹ nil),
  stays owner-exclusive through the window (the same rule that protects
  Trying — no other thread may read a non-Valid slot's `.Val`), and the commit
  is release-store(entry) **then** CAS Shadow→Valid — the transient
  Shadow-with-entry window this creates is unobservable by the ownership rule
  and harmless by mechanism (the commit CAS bumps the seqlock version;
  a racing DWCAS compares a pointer captured under a Valid window and fails).
  In Go the abort therefore needs no cleanup — a single CAS Shadow→Invalid —
  whereas the **spec model's** abort is two-phase (LC7 Shadow→Trying posts the
  result; LC9 clears the ghost val/tag, Trying→Invalid) purely because the
  ghost placeholder must be cleared.
  Shadow-uniqueness must come from the key, never the pointer. The two
  contract corollaries (at-most-once fn per key, holder exclusivity) are
  checked in `verify.sh` stage 4 as `inv ⟹ contract` (0-step anchored,
  2-proc).
- Every CAS / seqlock check compares a whole `Header` record
  (`{version, slotState}`), modeling the implementation's full-64-bit header CAS.

**Resize is not yet modeled** (it is the next phase of work — see
`docs/superpowers/specs/2026-06-05-phase0-inductive-quint-spec-design.md` §9).

## What is proved (and how)

1. **Inductive invariant** (`invariants.qnt::inv`, a conjunction of `typeOK`
   plus 27 families). Proved *inductive*, not merely bounded:
   - **Base:** `init ⟹ inv`, checked as `quint verify verify/invariants_<cfg>
     --invariant=inv --max-steps=0`.
   - **Consecution:** `inv ∧ step ⟹ inv'`, checked as `quint verify
     verify/induction_<cfg> --invariant=inv --max-steps=1`, where
     `induction.qnt::indInit` constructs an *arbitrary* `inv`-satisfying state
     (the induction anchor).

   Configs: `1proc`, `2proc` (default), and `2proc_2key` (opt-in; see
   [Cost](#cost-and-tiers)). All at 1 bin, 3 slots, valDomain {v1,v2}.

   Every family carries a `//` comment naming the protocol mechanism that
   guarantees it — `inv` *is* the protocol's written, machine-checked proof.

2. **Step refinement against the atomic map** (`refinement.qnt` vs
   `types.qnt::deltaResult`). The abstraction function reads each key's visible
   value; each concrete step's witness must be reproducible by ≤ `refineBound`
   abstract moves. Checked two ways:
   - **Anchored** (`refinement_anchored_<cfg>`, via `refineIndInit`): from
     *every* `inv`-state — depth-independent. With (1), this covers every
     reachable step at these configs: the linearizability evidence.
   - **From-init bounded** (`refinement_<cfg>`): a cheaper sanity layer that
     also exercises `init`.

3. **Simulator layer**: random traces (`quint run`, depth ≥ 20, ≥ 10 000
   samples) check `typeOK` and `inv` on reachable states — the cheap
   "reachable direction" that guards against over-strengthening.

4. **Directed scenarios** (`tests/scenarios.qnt`): 32 hand-written interleavings
   (contention, delete-window, shadow hand-off, multi-key, LC5/LC7 CAS-failure
   cleanup, LC abort handoff / LC9-window overlap / D8 version-silent drift),
   17 of which also assert `inv` in their final state. A separate
   `tests/scenarios_2bin.qnt` (3 runs) re-instantiates the protocol at
   `numBins = 2` to exercise cross-bin isolation, which is vacuous at 1 bin.

## What is NOT proved

- **Memory model**: the spec is sequentially consistent. Fence placement, torn
  reads, and the seqlock's plain-load arguments live in `design.md` and are
  exercised empirically by the Porcupine linearizability tests (run **without**
  `-race` — see the repo's `tests/`).
- **Domain sizes**: 1 bin, 3 slots, ≤ 2 procs, ≤ 2 keys, 2 values. Bugs that
  need larger instances escape (standard small-scope hypothesis). A 2-bin
  config exists (`verify/invariants_2proc_2key_2bin.qnt`) but only at the
  simulator + scenarios tiers — induction still runs at 1 bin.
- **Resize**: unmodeled until the next phase.
- **Insert's keep-slot retry path**: on finalize failure, the Go implementation
  (`allocator/insert.go:102–153`, `retryWithSlot`) KEEPS its `Trying`
  reservation, re-scans under a fresh header `h2`, and re-finalizes with `h2`
  as the CAS expectation; the spec models unreserve-and-restart (I6 → I1)
  instead. This is a sound coarsening for linearizability — every keep-slot
  outcome maps to an unreserve+retry execution with identical abstract
  behavior — but the re-finalize-under-fresh-header edge itself is NOT
  machine-verified. Its correctness rests on the header's ABA-freedom:
  versions only grow, and the only version-silent header write (D8's AND)
  cannot recreate a prior bit pattern without intervening bumps. Modeling it
  (new PCs I7/I8 + anchor/`allPCs`/space-table growth + full re-verification)
  is a candidate next work item.
- **Liveness**: every check is safety; nothing is proved to terminate.

## Anchor completeness — the one thing to be paranoid about

Consecution is sound only if `indInit` ranges over **all** `inv`-states.
Under-coverage passes *silently* (a non-inductive `inv` would be reported as
verified). Three controls defend this:

1. **Int types are closed in `inv` itself** (e.g. `tagMonotonic` bounds tags to
   `[1, nextTag)`), so the anchor's `Int` codomains are provably complete.
2. The **per-variable space table** in `induction.qnt` documents each
   constructed space against its variable's declared type. *When you add a
   variable or a PC variant, update `indInit` + `allPCs` + the table in the same
   commit.*
3. **Negative controls** (recorded below) prove both the invariant wiring and
   the anchor breadth are load-bearing.

### Negative-control experiment

Two controls, run on `induction_1proc` (1-proc consecution, `--max-steps=1`),
prove the induction is neither vacuous nor anchor-blind. Both are temporary
edits, reverted afterward.

1. **Vacuity** — delete one load-bearing conjunct (`ownerSlotStateConsistent`)
   from `invPure`, keep the full anchor → consecution **`[violation]`** (the
   PcI4 fill step writes val/tag into an Invalid slot, breaking
   `invalidSlotClean`). Proves the conjunct is doing real work — the induction
   does not pass vacuously.

2. **Coverage** — keep that conjunct deleted *and* narrow the anchor by dropping
   the two fill PCs (`PcI4`, `PcLC4`) from `allPCs` → consecution **`[ok]` — a
   FALSE PASS.** The same non-inductive `inv` is now reported "verified" purely
   because the anchor can no longer construct the breaking state. This is the
   silent unsoundness the anchor-completeness controls defend against: an
   under-covered anchor turns a real counterexample invisible.

The minimal, self-contained version of control 2 lives in
`spec/probes/undercover.qnt`. Takeaway: when you add a variable or PC variant,
the anchor (`indInit` / `allPCs`) and its space-table comment MUST grow with it,
or coverage silently shrinks.

## Cost and tiers

Apalache consecution grows steeply with the config. `verify.sh` tiers it:

| Check | Wall time (observed) | Default |
|---|---|---|
| 1-proc consecution | ~1.5–9 min | always |
| 1-proc anchored refinement | ~5 min | always |
| 2-proc consecution | ~45 min – 2.8 h | `DLHT_VERIFY_2PROC=1` (default on) |
| 2-proc anchored refinement | ~21 min | `DLHT_VERIFY_2PROC=1` (default on) |
| 2-proc-2-key consecution | ~23 min (idle, 16 GB heap) | `DLHT_VERIFY_2KEY=1` (default **off**) |

Times vary widely with machine load (Apalache is SMT-bound; a 16 GB JVM heap is
set in `verify.sh` — an earlier 2-key attempt on a loaded machine with the
default 4 GB heap was still unfinished after ~1.5 h). The 2-proc consecution is
the dominant cost.

The 2-key check **must pass when enabled**; it is gated off by default only
because of its runtime. It was last run and **passed on 2026-06-11**
(`[ok]`, 1395626 ms ≈ 23 min, quint 0.32.0 / Apalache 0.56.1, 16 GB heap). Set
`DLHT_VERIFY_2PROC=0` for a fast (~few-minute) smoke that still runs 1-proc
induction, anchored refinement, the simulator, and all scenarios.

Full-suite wall time (observed, per stage, this machine):
- **Fast tier** (`DLHT_VERIFY_2PROC=0`): ~6–15 min (observed 372 s end-to-end)
  — typechecks + simulator + anchor smoke + 1-proc
  base/consecution/anchored/bounded + scenarios.
- **Default tier** (`DLHT_VERIFY_2PROC=1`): ~1.5–3 h, dominated by the 2-proc
  consecution (~45 min – 2.8 h alone) plus 2-proc anchored (~21 min).
- **Full assurance** (`DLHT_VERIFY_2KEY=1`): add the multi-hour 2-key
  consecution.

## A quint gotcha worth knowing

`quint test tests/scenarios.qnt` (a bare invocation) runs **zero** of the 32
scenarios: quint's default test selection only runs `run` definitions whose
*name* matches a "test" pattern, and the scenarios are descriptively named.
`verify.sh` uses `--match '_.*_'` (selects exactly the snake_case scenario runs,
excludes the single-underscore `unchanged_*` protocol actions) and asserts the
exact counts (`32 passing`, and `3 passing` for `tests/scenarios_2bin.qnt`) so
coverage cannot silently shrink.

## Toolchain

- **quint 0.32.0** — installed from the GitHub release binary (npm lags at
  0.31.0); pinned by `verify.sh`'s version gate.
- **Apalache 0.56.1** — auto-managed by quint under `~/.quint/`.
- Two Apalache `quint verify` runs share one gRPC server and cancel each other —
  never run them concurrently. `quint test` / `quint typecheck` / `quint run`
  use the Rust evaluator and are safe to run anytime.

## Probes

`spec/probes/` holds the capability probes (P1, P1b, P2, P3, P4-export) and the
induction-semantics probes (`indsem`, `step0`, `vacuity`, `undercover`) that
validate the verification mechanisms themselves. Re-run them after any toolchain
bump.
