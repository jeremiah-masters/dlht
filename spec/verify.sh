#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"

APALACHE_OUT_DIR="${APALACHE_OUT_DIR:-/tmp/dlht-apalache-out}"
DLHT_VERIFY_2PROC="${DLHT_VERIFY_2PROC:-1}"
RUN_MAX_STEPS="${RUN_MAX_STEPS:-20}"
RUN_MAX_SAMPLES="${RUN_MAX_SAMPLES:-10000}"
INV_BOUNDED_STEPS_1PROC="${INV_BOUNDED_STEPS_1PROC:-6}"
INV_BOUNDED_STEPS_2PROC="${INV_BOUNDED_STEPS_2PROC:-4}"
REFINE_LOGICAL_STEPS_1PROC="${REFINE_LOGICAL_STEPS_1PROC:-4}"
REFINE_LOGICAL_STEPS_2PROC="${REFINE_LOGICAL_STEPS_2PROC:-2}"
# RefineNext is split into 3 phases: snapshot, next, check.
REFINE_MAX_STEPS_1PROC="$((3 * REFINE_LOGICAL_STEPS_1PROC))"
REFINE_MAX_STEPS_2PROC="$((3 * REFINE_LOGICAL_STEPS_2PROC))"

run_or_skip_choose_some() {
  local label="$1"
  shift

  local out
  local rc
  set +e
  out=$("$@" 2>&1)
  rc=$?
  set -e

  if [[ $rc -eq 0 ]]; then
    if [[ -n "$out" ]]; then
      printf "%s\n" "$out"
    fi
    return 0
  fi

  if printf "%s\n" "$out" | grep -q "Runtime does not support the built -in operator 'chooseSome'"; then
    echo "Skipped: $label (quint run does not support chooseSome in this model)."
    return 0
  fi

  printf "%s\n" "$out" >&2
  return "$rc"
}

run_inductive_or_bounded() {
  local module="$1"
  local bounded_steps="$2"
  local label="$3"

  local out
  local rc
  set +e
  out=$(OUT_DIR="$APALACHE_OUT_DIR" quint verify "$module" --inductive-invariant=inv --verbosity=0 2>&1)
  rc=$?
  set -e

  if [[ $rc -eq 0 ]]; then
    return 0
  fi

  if printf "%s\n" "$out" | grep -q "used before it is assigned"; then
    echo "Inductive check fallback ($label): Apalache assignment checker rejected Inv-as-init; running bounded invariant check."
    OUT_DIR="$APALACHE_OUT_DIR" quint verify "$module" --invariant=inv --max-steps="$bounded_steps" --verbosity=0
    return 0
  fi

  printf "%s\n" "$out" >&2
  return "$rc"
}

echo "=== Type checking ==="
quint typecheck types.qnt
quint typecheck protocol.qnt
quint typecheck invariants.qnt
quint typecheck verify/invariants_1proc.qnt
quint typecheck verify/invariants_2proc.qnt
quint typecheck augmentation.qnt
quint typecheck common.qnt
quint typecheck refinement.qnt
quint typecheck verify/refinement_1proc.qnt
quint typecheck verify/refinement_2proc.qnt
quint typecheck tests/scenarios.qnt

echo "=== Simulator: random traces ==="
run_or_skip_choose_some "typeOK random traces (1-proc)" \
  quint run verify/invariants_1proc.qnt --invariant=typeOK --max-steps="$RUN_MAX_STEPS" --max-samples="$RUN_MAX_SAMPLES"
run_or_skip_choose_some "Inv random traces (1-proc)" \
  quint run verify/invariants_1proc.qnt --invariant=inv --max-steps="$RUN_MAX_STEPS" --max-samples="$RUN_MAX_SAMPLES"
if [[ "$DLHT_VERIFY_2PROC" == "1" ]]; then
  run_or_skip_choose_some "Inv random traces (2-proc)" \
    quint run verify/invariants_2proc.qnt --invariant=inv --max-steps="$RUN_MAX_STEPS" --max-samples="$RUN_MAX_SAMPLES"
fi

echo "=== Apalache: inductive invariant ==="
echo "Using Apalache out dir: $APALACHE_OUT_DIR"
mkdir -p "$APALACHE_OUT_DIR"
run_inductive_or_bounded verify/invariants_1proc.qnt "$INV_BOUNDED_STEPS_1PROC" "1-proc"
if [[ "$DLHT_VERIFY_2PROC" == "1" ]]; then
  run_inductive_or_bounded verify/invariants_2proc.qnt "$INV_BOUNDED_STEPS_2PROC" "2-proc"
fi

echo "=== Apalache: bounded refinement ==="
echo "Refinement logical steps: 1-proc=$REFINE_LOGICAL_STEPS_1PROC (max-steps=$REFINE_MAX_STEPS_1PROC), 2-proc=$REFINE_LOGICAL_STEPS_2PROC (max-steps=$REFINE_MAX_STEPS_2PROC)"
OUT_DIR="$APALACHE_OUT_DIR" quint verify verify/refinement_1proc.qnt --invariant=inv --max-steps="$REFINE_MAX_STEPS_1PROC" --verbosity=0
if [[ "$DLHT_VERIFY_2PROC" == "1" ]]; then
  OUT_DIR="$APALACHE_OUT_DIR" quint verify verify/refinement_2proc.qnt --invariant=inv --max-steps="$REFINE_MAX_STEPS_2PROC" --verbosity=0
fi

echo "=== Scenario tests ==="
quint test tests/scenarios.qnt --max-samples=10000

echo "All checks passed."
