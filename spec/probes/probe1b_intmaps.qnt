// P1b: Int (not just Nat) codomains in setOfMaps + scalar Int.oneOf(),
// the exact shapes induction.qnt's indInit uses (Int = maximally robust
// anchor space; inv's own bounds do the cutting).
module probe1b_intmaps {
  var f: int -> int
  var n: int

  action init = {
    nondet m = Set(1, 2).setOfMaps(Int).oneOf()
    nondet x = Int.oneOf()
    all {
      f' = m,
      n' = x,
    }
  }

  action step = all {
    f' = f.set(1, f.get(1) + 1),
    n' = n + 1,
  }

  // No lower-bound claims — Int really is unconstrained. Both clauses are
  // intentional tautologies: the probe tests that the toolchain ACCEPTS Int
  // codomains (nondet assign -> read -> arithmetic -> re-assign), not that it
  // bounds them. A violation here is impossible; any exit-0 is a genuine
  // capability confirmation.
  def inv = f.get(1) >= f.get(1) and n >= n
}
