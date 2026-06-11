// P3: Does --inductive-invariant work when inv LEADS with binding conjuncts
// (x.in(S), including a setOfMaps(Nat) binding)? Tests whether the quint-native
// flag is viable once bindings exist — for variables that CAN express their space.
module probe3_indflag {
  var f: int -> int
  var st: int -> int

  val dom = Set(1, 2)

  action init = all {
    f' = dom.mapBy(_ => 0),
    st' = dom.mapBy(_ => 0),
  }

  action step = all {
    f' = f.set(1, f.get(1) + 1),
    st' = st,
  }

  def inv = and {
    // binding conjuncts first (assignment-checker food)
    f.in(dom.setOfMaps(Nat)),
    st.in(dom.setOfMaps(0.to(2))),
    // then the real invariant content
    f.get(1) >= 0,
  }
}
