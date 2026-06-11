#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"

# --- Toolchain gate -----------------------------------------------------
# Pinned after the silent 0.29->0.31 drift broke the chooseSome skip-grep and
# masked that --inductive-invariant had never actually run (fell back to
# bounded checks). Bump deliberately: change the pin, re-run the probes in
# spec/probes/, then the full suite.
# Apalache dist observed with this quint: apalache-dist-0.56.1
DLHT_QUINT_VERSION="${DLHT_QUINT_VERSION:-0.32.0}"
if ! actual_quint_version="$(quint --version 2>&1)"; then
  echo "FATAL: quint not found or crashed. Install quint ${DLHT_QUINT_VERSION}." >&2
  exit 1
fi
if [[ "$actual_quint_version" != "$DLHT_QUINT_VERSION" ]]; then
  echo "FATAL: quint $actual_quint_version found, pinned $DLHT_QUINT_VERSION." >&2
  echo "Install the pinned version or override: DLHT_QUINT_VERSION=$actual_quint_version ./verify.sh" >&2
  exit 1
fi
echo "Toolchain: quint $actual_quint_version (pinned)"

APALACHE_OUT_DIR="${APALACHE_OUT_DIR:-/tmp/dlht-apalache-out}"
export OUT_DIR="$APALACHE_OUT_DIR"
mkdir -p "$APALACHE_OUT_DIR"

# Apalache's launcher defaults to only -Xmx4096m; give it a larger heap (the
# consecution checks are heavy). Respects an externally-set JVM_ARGS.
export JVM_ARGS="${JVM_ARGS:--Xmx16384m}"

# Cost tiers. Apalache consecution is expensive and grows fast with the config:
# 1-proc ~1.5min, 2-proc ~45min, 2-proc-2-key hours. The 1-proc checks always
# run; the heavier configs are opt-in.
#   DLHT_VERIFY_2PROC=1  (default) 2-proc base+consecution+refinement (~45min+)
#   DLHT_VERIFY_2KEY=0   (default) 2-proc-2-key base+consecution (hours) — must
#                        pass when enabled; run it before claiming full assurance.
DLHT_VERIFY_2PROC="${DLHT_VERIFY_2PROC:-1}"
DLHT_VERIFY_2KEY="${DLHT_VERIFY_2KEY:-0}"

# Simulator depths are MINIMUMS, not suggestions: shallow runs never reach the
# deep action paths and would green-wash this stage. Do not lower them.
RUN_MAX_STEPS="${RUN_MAX_STEPS:-20}"
RUN_MAX_SAMPLES="${RUN_MAX_SAMPLES:-10000}"
# Anchored refinement: 3 raw steps = 1 logical step (snapshot->execute->check).
REFINE_ANCHORED_STEPS="${REFINE_ANCHORED_STEPS:-3}"
# From-init bounded refinement (sanity): logical steps x3 raw phases.
REFINE_INIT_STEPS_1PROC="${REFINE_INIT_STEPS_1PROC:-12}"
REFINE_INIT_STEPS_2PROC="${REFINE_INIT_STEPS_2PROC:-6}"
# Number of scenario run-definitions expected (snake_case names; --match '_.*_'
# selects exactly these and excludes the single-underscore unchanged_* actions).
SCENARIO_COUNT="${SCENARIO_COUNT:-32}"
SCENARIO_2BIN_COUNT="${SCENARIO_2BIN_COUNT:-3}"

verify() { OUT_DIR="$APALACHE_OUT_DIR" quint verify "$@" --verbosity=0; }

echo "=== 1. Type checking ==="
for m in types.qnt protocol.qnt invariants.qnt induction.qnt checked.qnt \
         common.qnt augmentation.qnt refinement.qnt \
         verify/invariants_1proc.qnt verify/invariants_2proc.qnt verify/invariants_2proc_2key.qnt \
         verify/invariants_2proc_2key_2bin.qnt \
         verify/induction_1proc.qnt verify/induction_2proc.qnt verify/induction_2proc_2key.qnt \
         verify/refinement_1proc.qnt verify/refinement_2proc.qnt \
         verify/refinement_anchored_1proc.qnt verify/refinement_anchored_2proc.qnt \
         tests/scenarios.qnt tests/scenarios_2bin.qnt; do
  quint typecheck "$m"
done

echo "=== 2. Simulator: random traces (typeOK + inv) ==="
for cfg in invariants_1proc invariants_2proc invariants_2proc_2key invariants_2proc_2key_2bin; do
  quint run "verify/$cfg.qnt" --invariant=typeOK --max-steps="$RUN_MAX_STEPS" --max-samples="$RUN_MAX_SAMPLES"
  quint run "verify/$cfg.qnt" --invariant=inv --max-steps="$RUN_MAX_STEPS" --max-samples="$RUN_MAX_SAMPLES"
done

echo "=== 3. Anchor smoke (a satisfying inv-state exists) ==="
verify verify/induction_1proc.qnt --invariant=smoke --max-steps=0

echo "=== 4. Induction: base + consecution ==="
echo "  [1-proc] base + consecution"
verify verify/invariants_1proc.qnt --invariant=inv --max-steps=0
verify verify/induction_1proc.qnt --invariant=inv --max-steps=1
if [[ "$DLHT_VERIFY_2PROC" == "1" ]]; then
  echo "  [2-proc] base + consecution (~45min)"
  verify verify/invariants_2proc.qnt --invariant=inv --max-steps=0
  verify verify/induction_2proc.qnt --invariant=inv --max-steps=1
  # Contract corollaries (invariants.qnt): NOT invPure conjuncts — checked as
  # inv => corollary. 0-step from indInit covers ALL inv-states; 2-proc is the
  # meaningful config (at 1-proc at-most-once-per-key is trivially true).
  echo "  [2-proc] contract corollaries (inv => contract, 0-step)"
  verify verify/induction_2proc.qnt --invariant=f_contractAtMostOneComputePerKey --max-steps=0
  verify verify/induction_2proc.qnt --invariant=f_contractHolderExclusive --max-steps=0
else
  echo "  [2-proc] SKIPPED (DLHT_VERIFY_2PROC=0)"
fi
if [[ "$DLHT_VERIFY_2KEY" == "1" ]]; then
  echo "  [2-proc-2-key] base + consecution (hours)"
  verify verify/invariants_2proc_2key.qnt --invariant=inv --max-steps=0
  verify verify/induction_2proc_2key.qnt --invariant=inv --max-steps=1
else
  echo "  [2-proc-2-key] SKIPPED (DLHT_VERIFY_2KEY=0; run with =1 for full assurance)"
fi

echo "=== 5. Refinement: anchored (depth-independent) ==="
verify verify/refinement_anchored_1proc.qnt --invariant=inv --max-steps="$REFINE_ANCHORED_STEPS"
if [[ "$DLHT_VERIFY_2PROC" == "1" ]]; then
  verify verify/refinement_anchored_2proc.qnt --invariant=inv --max-steps="$REFINE_ANCHORED_STEPS"
else
  echo "  [anchored 2-proc] SKIPPED (DLHT_VERIFY_2PROC=0)"
fi

echo "=== 6. Refinement: from-init bounded (sanity) ==="
verify verify/refinement_1proc.qnt --invariant=inv --max-steps="$REFINE_INIT_STEPS_1PROC"
if [[ "$DLHT_VERIFY_2PROC" == "1" ]]; then
  verify verify/refinement_2proc.qnt --invariant=inv --max-steps="$REFINE_INIT_STEPS_2PROC"
else
  echo "  [bounded 2-proc] SKIPPED (DLHT_VERIFY_2PROC=0)"
fi

echo "=== 7. Scenario tests ==="
# quint test only runs run-defs whose name matches its default 'test' pattern;
# the DLHT scenarios are descriptively named (no 'test'), so a bare invocation
# silently runs ZERO of them. --match '_.*_' selects exactly the snake_case
# scenario runs. Assert the exact count so a future drift can't silently shrink
# coverage.
# `|| true`: quint test exits non-zero on any scenario failure; without this the
# `set -e` would abort here (swallowing $scen_out) before the checks below run.
scen_out="$(quint test tests/scenarios.qnt --match '_.*_' --max-samples="$RUN_MAX_SAMPLES" 2>&1)" || true
echo "$scen_out" | grep -E "[0-9]+ passing|[0-9]+ failed" || true
if echo "$scen_out" | grep -qE "[0-9]+ failed"; then
  echo "FATAL: scenario test failure." >&2
  echo "$scen_out" | grep -A2 -E "[0-9]+\)" >&2 || true
  exit 1
fi
if ! echo "$scen_out" | grep -qE "(^| )${SCENARIO_COUNT} passing"; then
  echo "FATAL: expected exactly ${SCENARIO_COUNT} scenarios to run/pass (--match '_.*_')." >&2
  echo "Got: $(echo "$scen_out" | grep -E "passing" || echo "no passing line")" >&2
  exit 1
fi
echo "  ${SCENARIO_COUNT} scenarios passed."

# Cross-bin tier (numBins=2): same fail-closed pattern, separate count.
scen2_out="$(quint test tests/scenarios_2bin.qnt --match '_.*_' --max-samples="$RUN_MAX_SAMPLES" 2>&1)" || true
echo "$scen2_out" | grep -E "[0-9]+ passing|[0-9]+ failed" || true
if echo "$scen2_out" | grep -qE "[0-9]+ failed"; then
  echo "FATAL: 2-bin scenario test failure." >&2
  echo "$scen2_out" | grep -A2 -E "[0-9]+\)" >&2 || true
  exit 1
fi
if ! echo "$scen2_out" | grep -qE "(^| )${SCENARIO_2BIN_COUNT} passing"; then
  echo "FATAL: expected exactly ${SCENARIO_2BIN_COUNT} 2-bin scenarios to run/pass (--match '_.*_')." >&2
  echo "Got: $(echo "$scen2_out" | grep -E "passing" || echo "no passing line")" >&2
  exit 1
fi
echo "  ${SCENARIO_2BIN_COUNT} 2-bin scenarios passed."

echo "All checks passed."
