module refinement_anchored_1proc {
  // Depth-independent refinement: every protocol step from EVERY inv-state must
  // refine the abstract map. 3 raw steps = 1 logical step (snapshot -> execute
  // -> check), anchored at an arbitrary inv-state via refineIndInit.
  import refinement(
    procSet = Set("p1"),
    keyDomain = Set("k1"),
    valDomain = Set("v1", "v2"),
    numBins = 1,
    slotsPerBin = 3,
    hash = Map("k1" -> 1),
    refineBound = 2,
  ) as refine from "../refinement"

  action init = refine::refineIndInit
  action step = refine::refineNext
  def inv = refine::refinementStepOK
}
