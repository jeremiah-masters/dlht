module refinement {
  import types.* from "./types"
  import protocol.* from "./protocol"
  import common.* from "./common"
  import augmentation.* from "./augmentation"

  type RefinePhase = | PhaseSnapshot | PhaseExecute | PhaseCheck

  var stepOk: bool
  var refinePhase: RefinePhase

  // Snapshot of the pre-state for refinement checks.
  var preOp: PROC -> Op
  var preArg: PROC -> Option[Arg]
  var preRet: PROC -> Option[Res]
  var preHeader: BinIdx -> Header
  var preSlotKey: BinIdx -> (SlotIdx -> Option[KEY])
  var preSlotVal: BinIdx -> (SlotIdx -> Option[VAL])
  var preSlotTag: BinIdx -> (SlotIdx -> Option[int])

  const refineBound: int

  pure def witnessResFrom(opMap: PROC -> Op, retMap: PROC -> Option[Res]): PROC -> Option[Res] =
    procSet.mapBy(p => if (opMap.get(p) != None and retMap.get(p) != None) retMap.get(p) else None)

  pure def concreteStateFrom(headerMap: BinIdx -> Header, keyMap: BinIdx -> (SlotIdx -> Option[KEY]),
      valMap: BinIdx -> (SlotIdx -> Option[VAL]), tagMap: BinIdx -> (SlotIdx -> Option[int])): State =
    keyDomain.mapBy(k => readVisibleByHeaderFrom(headerMap.get(binOf(k)), keyMap, valMap, tagMap, binOf(k), k))

  pure def witnessSetFrom(headerMap: BinIdx -> Header, keyMap: BinIdx -> (SlotIdx -> Option[KEY]),
      valMap: BinIdx -> (SlotIdx -> Option[VAL]), tagMap: BinIdx -> (SlotIdx -> Option[int]),
      opMap: PROC -> Op, argMap: PROC -> Option[Arg], retMap: PROC -> Option[Res]): Set[Config] =
    Set({
      state: concreteStateFrom(headerMap, keyMap, valMap, tagMap),
      op: opMap,
      arg: argMap,
      res: witnessResFrom(opMap, retMap),
    })

  def sPre = witnessSetFrom(preHeader, preSlotKey, preSlotVal, preSlotTag, preOp, preArg, preRet)

  def sPost = witnessSetFrom(header, slotKey, slotVal, slotTag, op, arg, ret)

  def invokingProcsNow = procSet.filter(p => preOp.get(p) == None and op.get(p) != None)

  def returningProcsNow = procSet.filter(p => preOp.get(p) != None and op.get(p) == None)

  def intermediateStepNow = invokingProcsNow.size() == 0 and returningProcsNow.size() == 0

  def boundedRefinementCheck =
    (if (invokingProcsNow.size() > 0)
      {
        val p = invokingProcsNow.chooseSome()
        sPost.subseteq(invoke(sPre, p, op.get(p), arg.get(p)))
      }
    else true) and
      (if (intermediateStepNow)
        sPost.subseteq(boundedEvolve(sPre, refineBound))
      else true) and
      (if (returningProcsNow.size() > 0)
        {
          val p = returningProcsNow.chooseSome()
          sPost.subseteq(filterWitness(boundedEvolve(sPre, refineBound), p, ret.get(p)))
        }
      else true)

  action snapshotPre = all {
    refinePhase == PhaseSnapshot,
    pc' = pc,
    unchanged_control,
    unchanged_shared,
    unchanged_local,

    preOp' = op,
    preArg' = arg,
    preRet' = ret,
    preHeader' = header,
    preSlotKey' = slotKey,
    preSlotVal' = slotVal,
    preSlotTag' = slotTag,

    stepOk' = stepOk,
    refinePhase' = PhaseExecute,
  }

  action nextWithSnapshotFrame = all {
    refinePhase == PhaseExecute,
    step,
    preOp' = preOp,
    preArg' = preArg,
    preRet' = preRet,
    preHeader' = preHeader,
    preSlotKey' = preSlotKey,
    preSlotVal' = preSlotVal,
    preSlotTag' = preSlotTag,
    stepOk' = stepOk,
    refinePhase' = PhaseCheck,
  }

  action checkRefinement = all {
    refinePhase == PhaseCheck,
    pc' = pc,
    unchanged_control,
    unchanged_shared,
    unchanged_local,

    preOp' = preOp,
    preArg' = preArg,
    preRet' = preRet,
    preHeader' = preHeader,
    preSlotKey' = preSlotKey,
    preSlotVal' = preSlotVal,
    preSlotTag' = preSlotTag,

    stepOk' = (
      stepClassCover(invokingProcsNow.size() > 0, intermediateStepNow, returningProcsNow.size() > 0) and
        stepClassDisjoint(invokingProcsNow.size() > 0, intermediateStepNow, returningProcsNow.size() > 0) and
        boundedRefinementCheck
    ),
    refinePhase' = PhaseSnapshot,
  }

  action refineInit = all {
    init,
    stepOk' = true,

    preOp' = procSet.mapBy(_ => None),
    preArg' = procSet.mapBy(_ => None),
    preRet' = procSet.mapBy(_ => None),
    preHeader' = binDomain.mapBy(_ => emptyHeader),
    preSlotKey' = binDomain.mapBy(_ => slotIdx.mapBy(_ => None)),
    preSlotVal' = binDomain.mapBy(_ => slotIdx.mapBy(_ => None)),
    preSlotTag' = binDomain.mapBy(_ => slotIdx.mapBy(_ => None)),
    refinePhase' = PhaseSnapshot,
  }

  action refineNext = any {
    snapshotPre,
    nextWithSnapshotFrame,
    checkRefinement,
  }

  def refinementStepOK = stepOk
}
