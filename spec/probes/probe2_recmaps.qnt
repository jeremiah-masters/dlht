// P2: Can an init action CONSTRUCT a map-of-records-with-unbounded-int-field
// from separately chosen nondet maps (the Header shape: {version:int, slotState: map})?
// Plus a guard conjunct over the chosen values (the "state satisfies Inv" filter).
module probe2_recmaps {
  type H = { version: int, ss: int -> int }
  var hm: int -> H

  val bins = Set(1, 2)
  val slots = Set(1, 2, 3)

  action init = {
    nondet vers = bins.setOfMaps(Nat).oneOf()
    nondet sss = bins.setOfMaps(slots.setOfMaps(0.to(2))).oneOf()
    all {
      // guard over chosen values BEFORE assignment (pure predicate, no state vars)
      bins.forall(b => vers.get(b) >= 0),
      hm' = bins.mapBy(b => { version: vers.get(b), ss: sss.get(b) }),
    }
  }

  action step = all {
    hm' = hm.setBy(1, h => { ...h, version: h.version + 1 }),
  }

  def inv = bins.forall(b => hm.get(b).version >= 0)
}
