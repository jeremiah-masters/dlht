module augmentation {
  import types.* from "./types"

  pure def deltaImage(cPrev: Config, p: PROC): Set[Config] = {
    if (deltaEnabled(cPrev, p)) Set(deltaResult(cPrev, p)) else Set()
  }

  pure def invoke(P: Set[Config], p: PROC, newOp: Op, newArg: Option[Arg]): Set[Config] = {
    P.filter(cPrev =>
      cPrev.op.get(p) == None and cPrev.arg.get(p) == None and cPrev.res.get(p) == None
    ).map(cPrev => {
      ...cPrev,
      op: cPrev.op.set(p, newOp),
      arg: cPrev.arg.set(p, newArg),
      res: cPrev.res.set(p, None),
    })
  }

  pure def filterWitness(P: Set[Config], p: PROC, retVal: Option[Res]): Set[Config] = {
    P.filter(cPrev => cPrev.res.get(p) == retVal).map(cPrev => {
      ...cPrev,
      op: cPrev.op.set(p, None),
      arg: cPrev.arg.set(p, None),
      res: cPrev.res.set(p, None),
    })
  }

  pure def evolve1(P: Set[Config]): Set[Config] =
    P.map(cPrev => procSet.map(p => deltaImage(cPrev, p)).flatten()).flatten()

  pure def evolve2(P: Set[Config]): Set[Config] = evolve1(evolve1(P))

  pure def boundedEvolve(P: Set[Config], bound: int): Set[Config] = {
    if (bound == 0) P
    else if (bound == 1) P.union(evolve1(P))
    else P.union(evolve1(P)).union(evolve2(P))
  }

}
