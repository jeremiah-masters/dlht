// P1: Can Apalache assign a function-valued nondet with INFINITE (Nat) codomain?
// This is the core mechanism a hand-rolled inductive anchor (indInit) needs for
// per-bin version counters and nextTag.
module probe1_natmaps {
  var f: int -> int
  var n: int

  action init = {
    nondet m = Set(1, 2).setOfMaps(Nat).oneOf()
    all {
      f' = m,
      n' = 0,
    }
  }

  action step = all {
    f' = f.set(1, f.get(1) + 1),
    n' = n + 1,
  }

  def inv = f.get(1) >= 0 and f.get(2) >= 0
}
