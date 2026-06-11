module invariants_2proc_2key {
  import invariants(
    procSet = Set("p1", "p2"),
    keyDomain = Set("k1", "k2"),
    valDomain = Set("v1", "v2"),
    numBins = 1,
    slotsPerBin = 3,
    hash = Map("k1" -> 1, "k2" -> 1),
  ).* from "../invariants"

  action init = invariantsInit
  action step = invariantsStep
  def typeOK = invariantsTypeOK
}
