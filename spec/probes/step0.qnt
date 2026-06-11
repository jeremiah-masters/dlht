module step0 {
  var x: int
  // Anchor that VIOLATES inv at step 0 (guard does NOT enforce inv):
  action indInit = { nondet c = Nat.oneOf() all { c == 7, x' = c } }
  action step = all { x' = x }   // stutter; never breaks anything
  def inv = x <= 3   // x=7 violates this AT THE INITIAL STATE
}
