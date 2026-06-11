module induction_1proc {
  import induction(
    procSet = Set("p1"),
    keyDomain = Set("k1"),
    valDomain = Set("v1", "v2"),
    numBins = 1,
    slotsPerBin = 3,
    hash = Map("k1" -> 1),
  ).* from "../induction"

  action init = indInit
  action step = inductionStep
  def inv = inductionInv
  def smoke = anchorSat
}
