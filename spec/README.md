# DLHT Quint Spec — Verification Methodology

This directory formally models the DLHT lock-free hash table and verifies it with
[Quint](https://github.com/informalsystems/quint) + Apalache. Run everything with
`./verify.sh` (see [Toolchain](#toolchain) for the pinned versions).

## What is modeled

`protocol.qnt` models the DLHT operations — Get, Insert, Put, Delete, and
LoadOrCompute (with the `Shadow` slot state) — one atomic action per protocol
step (PC-indexed state machines: Get G1–G2, Insert I1–I6, Put P1–P7, Delete
D1–D8, LoadOrCompute LC1–LC10), over a **multi-generation flat bin
namespace** (design A′): generation *g* owns bins `2^g .. 2^(g+1)-1`,
`binFor(k, g)` is closed-form arithmetic, and configs pick `maxGen` ∈
{0, 1, 2} (maxGen=0 is the degenerate single-bin surface the Phase 0 results
live on).

`resize.qnt` models the **cooperative resize** as a daemon transfer machine
(design D2 — a safety over-approximation of Go's insert-triggered + FAA
work-stealing machinery): `rsTrigger` (allocate + indexNext publish),
`rsBegin` (InTransfer OR **without** a version bump + pre-OR snapshot),
`rsSentinel` (its own atomic step — the per-slot StoreLoad cut against
Put/Delete DWCAS), `rsMove` (fused read+place into the child; skips
concurrently-deleted entries), `rsShadowCarry` (the LoadOrCompute claim
carry — implemented in Go by `allocator/resize.go`'s `carryShadowSlots`),
`rsDone` (a snapshot **overwrite**, discarding transfer-window D8 ANDs;
sentinels persist in dead-bin slot keys forever), and `rsFinalize`
(`m.active` advances). `system.qnt` composes `systemStep = protocol step ∨
daemon step`. Ops classify `binState` per the design §5 normative table:
fused classification at every h0 load and D4 (Done → chain-walk exactly one
generation; InTransfer → reload `m.active`), **no check at P4** (the
pre-sentinel Put must remain carriable — put.go fidelity), unreserve refusal
at I6/LC8/LC9 (abandoned Trying slots), and the LC holder spin/relocate rule
(PcLC10) so a claim is never orphaned or committed mid-transfer.

Out of modeled scope (documented in the design, not silently dropped): chunk
work-stealing FAA bookkeeping and trigger timing (absorbed by the daemon
abstraction), link buckets/arena, the sentinel even/odd parity trick (the
modeled sentinel is type-distinct), Go's 8x/4x sizing tiers (growth factor 2),
`Range`/`Size`/`Stats`, weak memory (spec stays SC), liveness.

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
  (`{version, binState, slotState}`), modeling the implementation's
  full-64-bit header CAS — transfer-awareness auto-threads through every
  existing guard with zero per-guard edits.
- `slotKey: Option[HashKey]` (`HKey(KEY) | HSentinel`) is the DWCAS-compare
  hash word; `slotVal: Option[Entry]` carries the entry's OWN key, the ground
  truth for **abstract** visibility (design §6). Operational scans
  (candidate sets, P2/D2 slot picks) match the hash word — a sentineled slot
  is never a candidate; the abstraction reads each key at its
  **authoritative generation** (least non-Done bin in its chain).

Resize modeling follows
`docs/superpowers/specs/2026-06-11-phase1-resize-modeling-design.md`; the
stage-1a evidence tier is described below.

## What is proved (and how)

1. **Inductive invariant** (`invariants.qnt::inv`, a conjunction of `typeOK`
   plus 32 families — the 27 Phase 0 families, several adjusted for transfer
   shapes per design §7, plus the generation-topology tier R1–R5). Proved
   *inductive*, not merely bounded, **on the maxGen=0 configs**:
   - **Base:** `init ⟹ inv`, checked as `quint verify verify/invariants_<cfg>
     --invariant=inv --max-steps=0`.
   - **Consecution:** `inv ∧ step ⟹ inv'`, checked as `quint verify
     verify/induction_<cfg> --invariant=inv --max-steps=1`, where
     `induction.qnt::indInit` constructs an *arbitrary* `inv`-satisfying state
     (the induction anchor).

   Configs: `1proc`, `2proc` (default), and `2proc_2key` (opt-in; see
   [Cost](#cost-and-tiers)). All at maxGen=0 (one gen-0 bin), 3 slots,
   valDomain {v1,v2}.

   Every family carries a `//` comment naming the protocol mechanism that
   guarantees it — `inv` *is* the protocol's written, machine-checked proof.

1b. **Resize consecution (stage 1b) — `inv ∧ rsInv` INDUCTIVE.** `inv ∧ rsInv`
   is proved **inductive over `systemStep`** (protocol ∨ resize daemon) at
   **1-proc AND 2-proc** on `resize_1gen` (maxGen=1), via the system anchor
   `system_induction.qnt::systemIndInit` — which constructs an arbitrary state
   satisfying `invPure ∧ rsInvPure` *including* the daemon vars
   `snapshot`/`cursor` (the part 1a's anchor deliberately omitted); a per-family
   `g_<family>` harness localizes counterexamples, and a parallel runner
   (`consecution_sweep.sh`, one Apalache server per group via `--server-endpoint`)
   discharges the 2-proc goal group-by-group. The daemon-state families in
   `rsInvPure` (`rsSnapshotRelation` … `carriedClaimExists`, parameterized over
   snapshot/cursor and wrapped in `system.qnt`) are now **inductive conjuncts**,
   not merely reachable-true.
   - The 2-proc run surfaced and **closed a genuine inductiveness gap**:
     `shadowOwnedUnique` was non-inductive because `ownsShadowIn`'s carried-claim
     arm keys on key + gen-chain alone (not on the slot being a *live* Shadow),
     so the anchor could fabricate a dead-bin holder whose carried Shadow is
     absent and the arm latched onto another proc's same-key Trying reservation /
     Invalid stale-key slot (two counterexamples). **R23 `carriedClaimExists`**
     — a holder past a `DoneTransfer` own-gen bin *possesses* its carried child
     Shadow (true because `rsDone` only completes after `rsShadowCarry` copied
     every snapshot Shadow, per `rsCompleteness`) — makes it inductive. Diagnosis
     and fix independently confirmed by Codex + Opus reviews. The
     transfer-completeness negative control (neutralize `rsCompleteness` ⇒
     `[violation]`) confirms that family is load-bearing.
   - `resize_2gen` (maxGen=2) consecution exists and is gated under
     `DLHT_VERIFY_2GEN` (must pass when enabled).
   - Reachable-true backing remains: the simulator (`inv`+`rsInv` on both system
     configs, depth ≥1 full transfer cycle), base cases (verify.sh stage 4), and
     12 directed resize scenarios (`tests/scenarios_resize.qnt`): the sentinel
     cut from both sides, the delete window carried as absent + the straggler D8
     on a dead header, insert abandon-and-redirect in both orders, LC relocation
     (commit + abort), the snapStateAgrees resurrection interleaving, cross-bin
     isolation post-resize, and — on 2gen — the laggard double-walk and the
     holder double-relocation.
   Stage 1c (refinement / transfer-correctness — `rsDone` as an abstract no-op)
   remains future.

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
   17 of which also assert `inv` in their final state. Plus the 12 resize
   scenarios (`tests/scenarios_resize.qnt`, above). The former
   `tests/scenarios_2bin.qnt` is retired: A′'s gen-0 always has exactly one
   bin, so its cross-bin coverage re-landed post-resize (between gen-1's bins
   2/3, scenario `resize_cross_bin_isolated`) where it is strictly stronger.

## What is NOT proved

- **Memory model**: the spec is sequentially consistent. Fence placement, torn
  reads, and the seqlock's plain-load arguments live in `design.md` and are
  exercised empirically by the Porcupine linearizability tests (run **without**
  `-race` — see the repo's `tests/`).
- **Domain sizes**: ≤ 2 resizes (`maxGen ≤ 2`, 7 bins max), 2–3 slots per
  bin, ≤ 2 procs, ≤ 2 keys, 2 values. Bugs that need larger instances escape
  (standard small-scope hypothesis).
- **Resize, current bar (stage 1b done)**: `inv ∧ rsInv` is proved **inductive
  over the composed system** at 1-proc and 2-proc on `resize_1gen` (anchor +
  per-family sweep, above); `resize_2gen` consecution is gated. What remains is
  stage **1c** — anchored refinement over resize, i.e. the transfer-correctness
  theorem (`rsDone` is an abstract no-op). The Phase 0 inductive +
  anchored-refinement results continue to hold on the maxGen=0 configs
  (re-verified; `rsInvPure` additions are inert there by `genTopology`).
- **The keep-slot retry paths (Insert finalize, LoadOrCompute publish)**: on
  CAS failure, Go keeps the `Trying` reservation and rescans under a fresh
  header. It releases the slot if the key now exists or another claim holds
  the hash; otherwise it retries the CAS against that header. The spec models
  unreserve-and-restart (I6 → I1 and LC8 → LC1), so these Go retry paths are
  not machine-verified. They require the rescan and CAS to use the same
  header, including its version and bin state. Modeling the retry paths and
  re-verifying their refinement remains future work; the version-wraparound
  limit below also applies.
- **Entry pointer identity assumes GC no-reuse**: `slotTag`/`nextTag` model each
  entry pointer as never reused — sound for Go (a parked op's captured `*Entry`
  keeps the object reachable, so the allocator can't recycle it while a DWCAS
  still holds it as an expectation), but a port to manual reclamation or a
  recycling arena reintroduces pointer ABA the spec structurally cannot express.
- **`rsMove`'s fresh child tag is a control-flow *condition***: the daemon places
  transferred entries under a FRESH tag, over-approximating Go (which reuses the
  same `*Entry`). Sound ONLY because no Go retry loop spans an index generation
  (every DWCAS applies to the slot its expectation was read from); if a future
  optimization lets a parked Put/Delete chase its entry into the child while
  keeping the captured pointer, Go gains executions the spec excludes — re-verify.
- **32-bit version wraparound**: `version` is an unbounded `Int` here, 32 bits in
  Go. Every seqlock / ABA-freedom argument holds only modulo 2³² — a reader parked
  across exactly k·2³² bumps of one bin could false-validate `h0 == h1`. Standard,
  universally-accepted seqlock caveat.
- **Hash collisions**: `slotKey = HKey(KEY)` makes the hash word injective per
  key; two distinct keys with equal 64-bit maphash are inexpressible. Sound (Go
  rests on the `candidate.Key == key` re-check + pointer DWCAS, both modeled), but
  bin-level key↔hash collision interleavings are deliberately abstracted away.
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
| resize 1gen simulator (inv + rsInv, depth 40) | seconds–minutes | always |
| resize 2gen simulator pair | seconds–minutes | `DLHT_VERIFY_2GEN=1` (default **off**) |

The resize configs now participate in the **consecution tier** too (stage 1b):
`inv ∧ rsInv` inductive at 1-proc + 2-proc on `resize_1gen` (gated 2gen). Only
the refinement/transfer-correctness stage (1c) is still future.

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
exact counts (`32 passing`, and `12 passing` for `tests/scenarios_resize.qnt`)
so coverage cannot silently shrink.

Two more, learned in stage 1a: Apalache requires **constant integer bounds**
in `a.to(b)` and does not constant-fold `ApaFoldSet` — hence `pow2` is a
closed-form if-chain, not a range fold. And the Rust evaluator resolves a
`nondet x = S.oneOf()` **against the action's guards** (it searches for a
satisfying choice), which is what makes the daemon's nondet bin picks usable
in directed `.then(...)` chains.

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
