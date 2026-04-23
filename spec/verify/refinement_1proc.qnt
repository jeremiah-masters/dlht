module refinement_1proc {
  // 1-proc refinement smoke check.
  import refinement(
    procSet = Set("p1"),
    keyDomain = Set("k1"),
    valDomain = Set("v1", "v2"),
    numBins = 1,
    slotsPerBin = 3,
    hash = Map("k1" -> 1),
    refineBound = 2,
  ) as refine from "../refinement"

  action init = refine::refineInit
  action step = refine::refineNext
  def inv = refine::refinementStepOK
}
