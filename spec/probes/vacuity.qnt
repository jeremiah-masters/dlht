// Probe vacuity trap: if indInit's guard is UNSATISFIABLE (no anchor state
// exists), does consecution PASS VACUOUSLY (false "verified")?
module vacuity {
  var x: int
  action indInit = {
    nondet c = Nat.oneOf()
    all {
      c <= 3,
      c > 10,       // <-- UNSATISFIABLE conjunction: no c is both <=3 and >10
      x' = c,
    }
  }
  action step = any {
    all { x == 3, x' = 5 },   // would break inv if reachable
    all { x != 3, x' = x },
  }
  def inv = x <= 3
}
