// Probe: does `--init=indInit --invariant=inv --max-steps=1` faithfully encode
// CONSECUTION (inv(s) /\ step(s,s') => inv(s')) when indInit is a constructive
// guarded anchor (arbitrary typed state filtered by inv)?
//
// Setup: a counter x in 0..3 with a KNOWN inductive break.
//   step: x' = x + 1  (wraps via guard: only enabled if x < 3; at x==3, x'=x)
//   inv: x <= 3  -- this IS inductive under the real init (x=0) reachable set.
//   BUT we also test a NON-inductive candidate to confirm consecution catches it.
module indsem {
  var x: int

  // Real init
  action init = all { x' = 0 }

  // Constructive anchor: arbitrary Nat x, filtered by inv-guard.
  action indInit = {
    nondet c = Nat.oneOf()
    all {
      c <= 3,          // inv-guard BEFORE assignment
      x' = c,
    }
  }

  // A step that PRESERVES x<=3 only if guarded. Here: increment, but a buggy
  // branch lets x jump to 5 when x==3 (breaks induction at the boundary).
  action step = any {
    all { x < 3, x' = x + 1 },
    all { x == 3, x' = 5 },   // <-- inductive-break: from inv-state x=3, step to x=5 (not <=3)
  }

  def inv = x <= 3
}
