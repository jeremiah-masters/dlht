// THE central soundness hazard: if indInit under-covers the inv-state space,
// consecution can PASS even though inv is NOT inductive (a reachable inv-state
// that the anchor OMITS could step out of inv).
//
// Model: state (x, flag). inv = (x <= 3).  Reachable from init covers flag in {true,false}.
// Bug: when flag==false and x==3, step jumps to x=5 (breaks inv).
//      when flag==true, step is benign (x stays).
// If the ANCHOR only constructs flag==true states (under-coverage), consecution
// PASSES vacuously-for-the-bug, hiding the real break.
module undercover {
  var x: int
  var flag: bool

  // UNDER-COVERING anchor: forgets to nondet `flag`, hardcodes true.
  action indInitBad = {
    nondet c = Nat.oneOf()
    all { c <= 3, x' = c, flag' = true }   // flag==false states NEVER constructed
  }
  // FULL anchor: covers both flag values.
  action indInitGood = {
    nondet c = Nat.oneOf()
    nondet fl = Bool.oneOf()
    all { c <= 3, x' = c, flag' = fl }
  }

  action step = any {
    all { flag == false, x == 3, x' = 5, flag' = flag },  // the hidden break
    all { not(flag == false and x == 3), x' = x, flag' = flag },
  }
  def inv = x <= 3
}
