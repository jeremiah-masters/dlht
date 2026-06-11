module protocol {
  import types.* from "./types"

  const numBins: int
  const slotsPerBin: int
  const hash: KEY -> int

  type BinIdx = int
  type SlotIdx = int

  pure val binDomain: Set[BinIdx] = 1.to(numBins)
  pure val slotIdx: Set[SlotIdx] = 1.to(slotsPerBin)
  pure val slotIdxExt: Set[SlotIdx] = Set(0).union(slotIdx)

  type SlotState = | Invalid | Trying | Valid | Shadow
  type PC =
    | PcIdle
    | PcG1 | PcG2
    | PcI1 | PcI2 | PcI3 | PcI4 | PcI5 | PcI6
    | PcP1 | PcP2 | PcP3 | PcP4 | PcP5 | PcP6 | PcP7
    | PcD1 | PcD2 | PcD3 | PcD4 | PcD5 | PcD6 | PcD7 | PcD8
    | PcLC1 | PcLC2 | PcLC3 | PcLC4 | PcLC5 | PcLC6 | PcLC7 | PcLC8 | PcLC9

  type Header = {
    version: int,
    slotState: SlotIdx -> SlotState,
  }
  type LocalState = {
    h0: Header,
    targetSlot: SlotIdx,
    hLoop: Header,
    expectedVal: Option[VAL],
    expectedTag: Option[int],
  }

  var pc: PROC -> PC
  var op: PROC -> Op
  var arg: PROC -> Option[Arg]
  var ret: PROC -> Option[Res]

  var header: BinIdx -> Header
  var slotKey: BinIdx -> (SlotIdx -> Option[KEY])
  var slotVal: BinIdx -> (SlotIdx -> Option[VAL])
  var slotTag: BinIdx -> (SlotIdx -> Option[int])
  var nextTag: int

  var local: PROC -> LocalState

  // Snapshot of the full shared+control state. Pure invariant families take
  // this instead of reading vars, so the induction anchor (induction.qnt) can
  // evaluate them on *constructed* candidate states before assignment.
  type SpecState = {
    header: BinIdx -> Header,
    slotKey: BinIdx -> (SlotIdx -> Option[KEY]),
    slotVal: BinIdx -> (SlotIdx -> Option[VAL]),
    slotTag: BinIdx -> (SlotIdx -> Option[int]),
    nextTag: int,
    pc: PROC -> PC,
    op: PROC -> Op,
    arg: PROC -> Option[Arg],
    ret: PROC -> Option[Res],
    local: PROC -> LocalState,
  }

  def currentSpecState: SpecState = {
    header: header, slotKey: slotKey, slotVal: slotVal, slotTag: slotTag,
    nextTag: nextTag, pc: pc, op: op, arg: arg, ret: ret, local: local,
  }

  assume numBinsDef = numBins > 0
  assume slotsPerBinDef = slotsPerBin > 0
  assume hashDef = keyDomain.forall(k => binDomain.contains(hash.get(k)))
  assume finiteDomains =
    procSet.isFinite() and
    keyDomain.isFinite() and
    valDomain.isFinite() and
    procSet.size() > 0 and
    keyDomain.size() > 0 and
    valDomain.size() > 0

  pure val anyKey: KEY = pickAny(keyDomain)
  pure val anyVal: VAL = pickAny(valDomain)

  pure val emptyHeader: Header = {
    version: 0,
    slotState: slotIdx.mapBy(_ => Invalid),
  }
  pure val emptyLocal: LocalState = {
    h0: emptyHeader,
    targetSlot: 0,
    hLoop: emptyHeader,
    expectedVal: None,
    expectedTag: None,
  }

  pure def binOf(k: KEY): BinIdx = hash.get(k)

  pure def keyOfArg(a: Arg): KEY = {
    match a {
      | KeyArg(k) => k
      | KeyValArg(kv) => kv._1
      | LoadComputeArg(triple) => triple._1
    }
  }

  pure def valOfArg(a: Arg): VAL = {
    match a {
      | KeyValArg(kv) => kv._2
      | LoadComputeArg(triple) => triple._2
      | _ => anyVal
    }
  }

  pure def saveOfArg(a: Arg): bool = {
    match a {
      | LoadComputeArg(triple) => triple._3
      | _ => false
    }
  }

  def keyOfProc(p: PROC): KEY = {
    match arg.get(p) {
      | Some(a) => keyOfArg(a)
      | _ => anyKey
    }
  }

  def valOfProc(p: PROC): VAL = {
    match arg.get(p) {
      | Some(a) => valOfArg(a)
      | _ => anyVal
    }
  }

  def saveOfProc(p: PROC): bool = {
    match arg.get(p) {
      | Some(a) => saveOfArg(a)
      | _ => false
    }
  }

  pure def visibleSlotsByHeaderFrom(h: Header, keyMap: BinIdx -> (SlotIdx -> Option[KEY]),
      valMap: BinIdx -> (SlotIdx -> Option[VAL]), tagMap: BinIdx -> (SlotIdx -> Option[int]),
      b: BinIdx, k: KEY): Set[SlotIdx] =
    slotIdx.filter(s =>
      h.slotState.get(s) == Valid and
        keyMap.get(b).get(s) == Some(k) and
        valMap.get(b).get(s) != None and
        tagMap.get(b).get(s) != None
    )

  // Shadow slots only carry a key (value is a placeholder until LC7), so the
  // filter is key-only.
  pure def shadowSlotsByHeaderFrom(h: Header, keyMap: BinIdx -> (SlotIdx -> Option[KEY]),
      b: BinIdx, k: KEY): Set[SlotIdx] =
    slotIdx.filter(s =>
      h.slotState.get(s) == Shadow and
        keyMap.get(b).get(s) == Some(k)
    )

  def shadowSlotsByHeader(h: Header, b: BinIdx, k: KEY): Set[SlotIdx] =
    shadowSlotsByHeaderFrom(h, slotKey, b, k)

  pure def readVisibleByHeaderFrom(h: Header, keyMap: BinIdx -> (SlotIdx -> Option[KEY]),
      valMap: BinIdx -> (SlotIdx -> Option[VAL]), tagMap: BinIdx -> (SlotIdx -> Option[int]),
      b: BinIdx, k: KEY): Option[VAL] = {
    val visible = visibleSlotsByHeaderFrom(h, keyMap, valMap, tagMap, b, k)
    if (visible.size() == 0) None
    else {
      val s = minOr0(visible)
      valMap.get(b).get(s)
    }
  }

  def visibleSlotsByHeader(h: Header, b: BinIdx, k: KEY): Set[SlotIdx] =
    visibleSlotsByHeaderFrom(h, slotKey, slotVal, slotTag, b, k)

  def readVisibleByHeader(h: Header, b: BinIdx, k: KEY): Option[VAL] =
    readVisibleByHeaderFrom(h, slotKey, slotVal, slotTag, b, k)

  def slotForKeyByHeader(h: Header, b: BinIdx, k: KEY): SlotIdx = {
    minOr0(visibleSlotsByHeader(h, b, k))
  }

  pure def opFromPC(id: PC): Op = match id {
    | PcIdle => None
    | PcG1 => Some(Get)
    | PcG2 => Some(Get)
    | PcI1 => Some(Insert)
    | PcI2 => Some(Insert)
    | PcI3 => Some(Insert)
    | PcI4 => Some(Insert)
    | PcI5 => Some(Insert)
    | PcI6 => Some(Insert)
    | PcP1 => Some(Put)
    | PcP2 => Some(Put)
    | PcP3 => Some(Put)
    | PcP4 => Some(Put)
    | PcP5 => Some(Put)
    | PcP6 => Some(Put)
    | PcP7 => Some(Put)
    | PcD1 => Some(Delete)
    | PcD2 => Some(Delete)
    | PcD3 => Some(Delete)
    | PcD4 => Some(Delete)
    | PcD5 => Some(Delete)
    | PcD6 => Some(Delete)
    | PcD7 => Some(Delete)
    | PcD8 => Some(Delete)
    | PcLC1 => Some(LoadOrCompute)
    | PcLC2 => Some(LoadOrCompute)
    | PcLC3 => Some(LoadOrCompute)
    | PcLC4 => Some(LoadOrCompute)
    | PcLC5 => Some(LoadOrCompute)
    | PcLC6 => Some(LoadOrCompute)
    | PcLC7 => Some(LoadOrCompute)
    | PcLC8 => Some(LoadOrCompute)
    | PcLC9 => Some(LoadOrCompute)
  }

  pure def opStartLine(opname: OpName): PC = match opname {
    | Get => PcG1
    | Insert => PcI1
    | Put => PcP1
    | Delete => PcD1
    | LoadOrCompute => PcLC1
  }

  pure def updateSlotState(h: Header, s: SlotIdx, newState: SlotState): Header = {
    { version: h.version + 1, slotState: h.slotState.set(s, newState) }
  }

  pure def clearSlotState(h: Header, s: SlotIdx, newState: SlotState): Header = {
    { ...h, slotState: h.slotState.set(s, newState) }
  }

  pure def mkResult(ok: bool, v: Option[VAL]): Option[Res] = Some({ ok: ok, resVal: v })

  pure def withH0(ls: LocalState, h: Header): LocalState = {
    ...ls,
    h0: h,
  }

  pure def withTargetSlot(ls: LocalState, s: SlotIdx): LocalState = {
    ...ls,
    targetSlot: s,
  }

  pure def withHLoop(ls: LocalState, h: Header): LocalState = {
    ...ls,
    hLoop: h,
  }

  pure def withExpected(ls: LocalState, v: Option[VAL], t: Option[int]): LocalState = {
    ...ls,
    expectedVal: v,
    expectedTag: t,
  }

  action unchanged_control = all {
    op' = op,
    arg' = arg,
    ret' = ret,
  }

  action unchanged_shared = all {
    header' = header,
    slotKey' = slotKey,
    slotVal' = slotVal,
    slotTag' = slotTag,
    nextTag' = nextTag,
  }

  action unchanged_local = all {
    local' = local,
  }

  action init = all {
    pc' = procSet.mapBy(_ => PcIdle),
    op' = procSet.mapBy(_ => None),
    arg' = procSet.mapBy(_ => None),
    ret' = procSet.mapBy(_ => None),

    header' = binDomain.mapBy(_ => emptyHeader),
    slotKey' = binDomain.mapBy(_ => slotIdx.mapBy(_ => None)),
    slotVal' = binDomain.mapBy(_ => slotIdx.mapBy(_ => None)),
    slotTag' = binDomain.mapBy(_ => slotIdx.mapBy(_ => None)),
    nextTag' = 1,

    local' = procSet.mapBy(_ => emptyLocal),
  }

  action invokeLine(p) = {
    nondet newOp = opNames.oneOf()
    nondet newArg = argsOf(Some(newOp)).oneOf()
    all {
      pc.get(p) == PcIdle,
      pc' = pc.set(p, opStartLine(newOp)),
      op' = op.set(p, Some(newOp)),
      arg' = arg.set(p, Some(newArg)),
      ret' = ret.set(p, None),
      unchanged_shared,
      unchanged_local,
    }
  }

  // -------------------------------- Get -----------------------------------

  action G1(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    all {
      pc.get(p) == PcG1,
      local' = local.set(p, withH0(local.get(p), header.get(b))),
      pc' = pc.set(p, PcG2),
      unchanged_control,
      unchanged_shared,
    }
  }

  action G2(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    all {
      pc.get(p) == PcG2,
      if (g2ReturnGuard(p)) {
        val result = readVisibleByHeader(local.get(p).h0, b, k)
        all {
          ret' = ret.set(p, mkResult(result != None, result)),
          op' = op.set(p, None),
          arg' = arg.set(p, None),
          pc' = pc.set(p, PcIdle),
        }
      } else {
        all {
          pc' = pc.set(p, PcG1),
          ret' = ret,
          op' = op,
          arg' = arg,
        }
      },
      unchanged_shared,
      unchanged_local,
    }
  }

  // ------------------------------ Insert ----------------------------------

  action I1(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    all {
      pc.get(p) == PcI1,
      local' = local.set(p, withH0(local.get(p), header.get(b))),
      pc' = pc.set(p, PcI2),
      unchanged_control,
      unchanged_shared,
    }
  }

  action I2(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    val h = local.get(p).h0
    all {
      pc.get(p) == PcI2,
      if (header.get(b) != h) {
        all {
          pc' = pc.set(p, PcI1),
          local' = local,
          ret' = ret,
          op' = op,
          arg' = arg,
        }
      } else if (i2ReturnGuard(p)) {
        all {
          ret' = ret.set(p, mkResult(false, readVisibleByHeader(h, b, k))),
          op' = op.set(p, None),
          arg' = arg.set(p, None),
          pc' = pc.set(p, PcIdle),
          local' = local,
        }
      } else if (shadowSlotsByHeader(h, b, k).size() != 0) {
        // Shadow holder owns this key; back off until it resolves.
        all {
          pc' = pc.set(p, PcI1),
          local' = local,
          ret' = ret,
          op' = op,
          arg' = arg,
        }
      } else {
        val free = slotIdx.filter(s => h.slotState.get(s) == Invalid)
        if (free.size() != 0) {
          all {
            local' = local.set(p, withTargetSlot(local.get(p), minOr0(free))),
            pc' = pc.set(p, PcI3),
            ret' = ret,
            op' = op,
            arg' = arg,
          }
        } else {
          all {
            pc' = pc.set(p, PcI1),
            local' = local,
            ret' = ret,
            op' = op,
            arg' = arg,
          }
        }
      },
      unchanged_shared,
    }
  }

  action I3(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    val s = local.get(p).targetSlot
    all {
      pc.get(p) == PcI3,
      if (s != 0 and header.get(b) == local.get(p).h0 and header.get(b).slotState.get(s) == Invalid) {
        all {
          header' = header.set(b, updateSlotState(header.get(b), s, Trying)),
          pc' = pc.set(p, PcI4),
        }
      } else {
        all {
          header' = header,
          pc' = pc.set(p, PcI1),
        }
      },
      unchanged_control,
      slotKey' = slotKey,
      slotVal' = slotVal,
      slotTag' = slotTag,
      nextTag' = nextTag,
      unchanged_local,
    }
  }

  action I4(p) = {
    val k = keyOfProc(p)
    val v = valOfProc(p)
    val b = binOf(k)
    val s = local.get(p).targetSlot
    all {
      pc.get(p) == PcI4,
      slotKey' = slotKey.set(b, slotKey.get(b).set(s, Some(k))),
      slotVal' = slotVal.set(b, slotVal.get(b).set(s, Some(v))),
      slotTag' = slotTag.set(b, slotTag.get(b).set(s, Some(nextTag))),
      nextTag' = nextTag + 1,
      pc' = pc.set(p, PcI5),
      unchanged_control,
      header' = header,
      local' = local,
    }
  }

  action I5(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    val s = local.get(p).targetSlot
    all {
      pc.get(p) == PcI5,
      if (i5SuccessGuard(p)) {
        all {
          header' = header.set(b, updateSlotState(header.get(b), s, Valid)),
          ret' = ret.set(p, mkResult(true, None)),
          op' = op.set(p, None),
          arg' = arg.set(p, None),
          pc' = pc.set(p, PcIdle),
        }
      } else {
        all {
          header' = header,
          ret' = ret,
          op' = op,
          arg' = arg,
          pc' = pc.set(p, PcI6),
        }
      },
      slotKey' = slotKey,
      slotVal' = slotVal,
      slotTag' = slotTag,
      nextTag' = nextTag,
      unchanged_local,
    }
  }

  action I6(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    val s = local.get(p).targetSlot
    all {
      pc.get(p) == PcI6,
      if (s != 0 and header.get(b).slotState.get(s) == Trying and slotKey.get(b).get(s) == Some(k)) {
        all {
          header' = header.set(b, updateSlotState(header.get(b), s, Invalid)),
          slotVal' = slotVal.set(b, slotVal.get(b).set(s, None)),
          slotTag' = slotTag.set(b, slotTag.get(b).set(s, None)),
        }
      } else {
        all {
          header' = header,
          slotVal' = slotVal,
          slotTag' = slotTag,
        }
      },
      pc' = pc.set(p, PcI1),
      unchanged_control,
      slotKey' = slotKey,
      nextTag' = nextTag,
      unchanged_local,
    }
  }

  // -------------------------------- Put -----------------------------------

  action P1(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    all {
      pc.get(p) == PcP1,
      local' = local.set(p, withH0(local.get(p), header.get(b))),
      pc' = pc.set(p, PcP2),
      unchanged_control,
      unchanged_shared,
    }
  }

  action P2(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    all {
      pc.get(p) == PcP2,
      local' = local.set(p, withTargetSlot(local.get(p), slotForKeyByHeader(local.get(p).h0, b, k))),
      pc' = pc.set(p, PcP3),
      unchanged_control,
      unchanged_shared,
    }
  }

  action P3(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    all {
      pc.get(p) == PcP3,
      if (header.get(b) != local.get(p).h0) {
        all {
          pc' = pc.set(p, PcP1),
          ret' = ret,
          op' = op,
          arg' = arg,
        }
      } else if (keyAbsentGuard(p)) {
        all {
          ret' = ret.set(p, mkResult(false, None)),
          op' = op.set(p, None),
          arg' = arg.set(p, None),
          pc' = pc.set(p, PcIdle),
        }
      } else {
        all {
          pc' = pc.set(p, PcP4),
          ret' = ret,
          op' = op,
          arg' = arg,
        }
      },
      unchanged_shared,
      unchanged_local,
    }
  }

  action P4(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    all {
      pc.get(p) == PcP4,
      local' = local.set(p, withHLoop(local.get(p), header.get(b))),
      pc' = pc.set(p, PcP5),
      unchanged_control,
      unchanged_shared,
    }
  }

  action P5(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    val s = local.get(p).targetSlot
    val retry =
      local.get(p).hLoop.slotState.get(s) != Valid or
        slotKey.get(b).get(s) != Some(k) or
        slotVal.get(b).get(s) == None
    all {
      pc.get(p) == PcP5,
      if (retry) {
        all {
          pc' = pc.set(p, PcP1),
          local' = local,
        }
      } else {
        all {
          pc' = pc.set(p, PcP6),
          local' = local.set(p, withExpected(local.get(p), slotVal.get(b).get(s), slotTag.get(b).get(s))),
        }
      },
      unchanged_control,
      unchanged_shared,
    }
  }

  action P6(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    all {
      pc.get(p) == PcP6,
      if (header.get(b) != local.get(p).hLoop) {
        pc' = pc.set(p, PcP4)
      } else {
        pc' = pc.set(p, PcP7)
      },
      unchanged_control,
      unchanged_shared,
      unchanged_local,
    }
  }

  action P7(p) = {
    val k = keyOfProc(p)
    val v = valOfProc(p)
    val b = binOf(k)
    val s = local.get(p).targetSlot
    all {
      pc.get(p) == PcP7,
      if (p7SuccessGuard(p)) {
        all {
          slotVal' = slotVal.set(b, slotVal.get(b).set(s, Some(v))),
          slotTag' = slotTag.set(b, slotTag.get(b).set(s, Some(nextTag))),
          nextTag' = nextTag + 1,
          ret' = ret.set(p, mkResult(true, local.get(p).expectedVal)),
          op' = op.set(p, None),
          arg' = arg.set(p, None),
          pc' = pc.set(p, PcIdle),
        }
      } else {
        all {
          slotVal' = slotVal,
          slotTag' = slotTag,
          nextTag' = nextTag,
          ret' = ret,
          op' = op,
          arg' = arg,
          pc' = pc.set(p, PcP4),
        }
      },
      header' = header,
      slotKey' = slotKey,
      local' = local,
    }
  }

  // ------------------------------- Delete ----------------------------------

  action D1(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    all {
      pc.get(p) == PcD1,
      local' = local.set(p, withH0(local.get(p), header.get(b))),
      pc' = pc.set(p, PcD2),
      unchanged_control,
      unchanged_shared,
    }
  }

  action D2(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    all {
      pc.get(p) == PcD2,
      local' = local.set(p, withTargetSlot(local.get(p), slotForKeyByHeader(local.get(p).h0, b, k))),
      pc' = pc.set(p, PcD3),
      unchanged_control,
      unchanged_shared,
    }
  }

  action D3(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    all {
      pc.get(p) == PcD3,
      if (header.get(b) != local.get(p).h0) {
        all {
          pc' = pc.set(p, PcD1),
          ret' = ret,
          op' = op,
          arg' = arg,
        }
      } else if (keyAbsentGuard(p)) {
        all {
          ret' = ret.set(p, mkResult(false, None)),
          op' = op.set(p, None),
          arg' = arg.set(p, None),
          pc' = pc.set(p, PcIdle),
        }
      } else {
        all {
          pc' = pc.set(p, PcD4),
          ret' = ret,
          op' = op,
          arg' = arg,
        }
      },
      unchanged_shared,
      unchanged_local,
    }
  }

  action D4(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    all {
      pc.get(p) == PcD4,
      local' = local.set(p, withHLoop(local.get(p), header.get(b))),
      pc' = pc.set(p, PcD5),
      unchanged_control,
      unchanged_shared,
    }
  }

  action D5(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    val s = local.get(p).targetSlot
    val retry =
      local.get(p).hLoop.slotState.get(s) != Valid or
        slotKey.get(b).get(s) != Some(k) or
        slotVal.get(b).get(s) == None
    all {
      pc.get(p) == PcD5,
      if (retry) {
        all {
          pc' = pc.set(p, PcD1),
          local' = local,
        }
      } else {
        all {
          pc' = pc.set(p, PcD6),
          local' = local.set(p, withExpected(local.get(p), slotVal.get(b).get(s), slotTag.get(b).get(s))),
        }
      },
      unchanged_control,
      unchanged_shared,
    }
  }

  action D6(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    all {
      pc.get(p) == PcD6,
      if (header.get(b) != local.get(p).hLoop) {
        pc' = pc.set(p, PcD4)
      } else {
        pc' = pc.set(p, PcD7)
      },
      unchanged_control,
      unchanged_shared,
      unchanged_local,
    }
  }

  action D7(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    val s = local.get(p).targetSlot
    all {
      pc.get(p) == PcD7,
      if (
        slotKey.get(b).get(s) == Some(k) and
        slotTag.get(b).get(s) == local.get(p).expectedTag and
        slotTag.get(b).get(s) != None and
        slotVal.get(b).get(s) != None
      ) {
        all {
          slotVal' = slotVal.set(b, slotVal.get(b).set(s, None)),
          slotTag' = slotTag.set(b, slotTag.get(b).set(s, None)),
          ret' = ret.set(p, mkResult(true, local.get(p).expectedVal)),
          pc' = pc.set(p, PcD8),
          op' = op,
          arg' = arg,
        }
      } else {
        all {
          slotVal' = slotVal,
          slotTag' = slotTag,
          ret' = ret,
          op' = op,
          arg' = arg,
          pc' = pc.set(p, PcD4),
        }
      },
      header' = header,
      slotKey' = slotKey,
      nextTag' = nextTag,
      local' = local,
    }
  }

  action D8(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    val s = local.get(p).targetSlot
    all {
      pc.get(p) == PcD8,
      if (header.get(b).slotState.get(s) == Valid and slotVal.get(b).get(s) == None and slotTag.get(b).get(s) == None) {
        header' = header.set(b, clearSlotState(header.get(b), s, Invalid))
      } else {
        header' = header
      },
      pc' = pc.set(p, PcIdle),
      op' = op.set(p, None),
      arg' = arg.set(p, None),
      ret' = ret,
      slotKey' = slotKey,
      slotVal' = slotVal,
      slotTag' = slotTag,
      nextTag' = nextTag,
      local' = local,
    }
  }

  // --------------------------- LoadOrCompute ------------------------------
  // Mirrors Insert; publishes to Shadow at LC5, runs fn while in Shadow, then
  // commits (Shadow -> Valid with fn's value) or aborts (Shadow -> Invalid)
  // at LC7.

  action LC1(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    all {
      pc.get(p) == PcLC1,
      local' = local.set(p, withH0(local.get(p), header.get(b))),
      pc' = pc.set(p, PcLC2),
      unchanged_control,
      unchanged_shared,
    }
  }

  action LC2(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    val h = local.get(p).h0
    all {
      pc.get(p) == PcLC2,
      if (header.get(b) != h) {
        all {
          pc' = pc.set(p, PcLC1),
          local' = local,
          ret' = ret,
          op' = op,
          arg' = arg,
        }
      } else if (lc2ReturnGuard(p)) {
        all {
          ret' = ret.set(p, mkResult(true, readVisibleByHeader(h, b, k))),
          op' = op.set(p, None),
          arg' = arg.set(p, None),
          pc' = pc.set(p, PcIdle),
          local' = local,
        }
      } else if (shadowSlotsByHeader(h, b, k).size() != 0) {
        // Wait for the holder.
        all {
          pc' = pc.set(p, PcLC1),
          local' = local,
          ret' = ret,
          op' = op,
          arg' = arg,
        }
      } else {
        val free = slotIdx.filter(s => h.slotState.get(s) == Invalid)
        if (free.size() != 0) {
          all {
            local' = local.set(p, withTargetSlot(local.get(p), minOr0(free))),
            pc' = pc.set(p, PcLC3),
            ret' = ret,
            op' = op,
            arg' = arg,
          }
        } else {
          all {
            pc' = pc.set(p, PcLC1),
            local' = local,
            ret' = ret,
            op' = op,
            arg' = arg,
          }
        }
      },
      unchanged_shared,
    }
  }

  action LC3(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    val s = local.get(p).targetSlot
    all {
      pc.get(p) == PcLC3,
      if (s != 0 and header.get(b) == local.get(p).h0 and header.get(b).slotState.get(s) == Invalid) {
        all {
          header' = header.set(b, updateSlotState(header.get(b), s, Trying)),
          pc' = pc.set(p, PcLC4),
        }
      } else {
        all {
          header' = header,
          pc' = pc.set(p, PcLC1),
        }
      },
      unchanged_control,
      slotKey' = slotKey,
      slotVal' = slotVal,
      slotTag' = slotTag,
      nextTag' = nextTag,
      unchanged_local,
    }
  }

  action LC4(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    val s = local.get(p).targetSlot
    all {
      pc.get(p) == PcLC4,
      // anyVal is a placeholder; LC7 overwrites it with fn's value (or clears it).
      slotKey' = slotKey.set(b, slotKey.get(b).set(s, Some(k))),
      slotVal' = slotVal.set(b, slotVal.get(b).set(s, Some(anyVal))),
      slotTag' = slotTag.set(b, slotTag.get(b).set(s, Some(nextTag))),
      nextTag' = nextTag + 1,
      pc' = pc.set(p, PcLC5),
      unchanged_control,
      header' = header,
      local' = local,
    }
  }

  action LC5(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    val s = local.get(p).targetSlot
    all {
      pc.get(p) == PcLC5,
      if (lc5SuccessGuard(p)) {
        all {
          header' = header.set(b, updateSlotState(header.get(b), s, Shadow)),
          pc' = pc.set(p, PcLC6),
        }
      } else {
        // Header drifted; unreserve and retry. fn has not run yet.
        all {
          header' = header,
          pc' = pc.set(p, PcLC8),
        }
      },
      unchanged_control,
      slotKey' = slotKey,
      slotVal' = slotVal,
      slotTag' = slotTag,
      nextTag' = nextTag,
      unchanged_local,
    }
  }

  action LC6(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    all {
      pc.get(p) == PcLC6,
      // Post-fn: reload hLoop for the commit CAS.
      local' = local.set(p, withHLoop(local.get(p), header.get(b))),
      pc' = pc.set(p, PcLC7),
      unchanged_control,
      unchanged_shared,
    }
  }

  action LC7(p) = {
    val k = keyOfProc(p)
    val v = valOfProc(p)
    val save = saveOfProc(p)
    val b = binOf(k)
    val s = local.get(p).targetSlot
    all {
      pc.get(p) == PcLC7,
      if (lc7SuccessGuard(p)) {
        if (save) {
          // Commit (linearization point for save=true).
          all {
            header' = header.set(b, updateSlotState(header.get(b), s, Valid)),
            slotVal' = slotVal.set(b, slotVal.get(b).set(s, Some(v))),
            slotTag' = slotTag.set(b, slotTag.get(b).set(s, Some(nextTag))),
            nextTag' = nextTag + 1,
            ret' = ret.set(p, mkResult(false, Some(v))),
            op' = op.set(p, None),
            arg' = arg.set(p, None),
            pc' = pc.set(p, PcIdle),
            local' = local,
          }
        } else {
          // Abort phase 1: Shadow -> Trying, and LINEARIZATION POINT for
          // save=false. The result (false, computed v) is fixed here, not at
          // the LC9 return: at this step the slot is still Shadow with key k,
          // so liveKeyUniquePerBin + shadowSlotKeyed guarantee k is absent from
          // the visible set, which is exactly the abstract precondition for the
          // "absent, fn ran, not saved" outcome. (Mirrors the save=true commit
          // above, which also linearizes at LC7.) LC9 is then pure cleanup.
          // The Shadow -> Trying flip keeps the slot invisible to L3/I2.5 scans,
          // so LC9 can clear slot.Val without exposing a Shadow-with-nil-entry
          // window to other LoadOrComputes.
          all {
            header' = header.set(b, updateSlotState(header.get(b), s, Trying)),
            slotVal' = slotVal,
            slotTag' = slotTag,
            nextTag' = nextTag,
            ret' = ret.set(p, mkResult(false, Some(v))),
            op' = op,
            arg' = arg,
            pc' = pc.set(p, PcLC9),
            local' = local,
          }
        }
      } else {
        // Header drifted; reload and retry.
        all {
          header' = header,
          slotVal' = slotVal,
          slotTag' = slotTag,
          nextTag' = nextTag,
          ret' = ret,
          op' = op,
          arg' = arg,
          pc' = pc.set(p, PcLC6),
          local' = local,
        }
      },
      slotKey' = slotKey,
    }
  }

  // LC8 mirrors I6: unreserve on LC5 failure, then retry from LC1.
  action LC8(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    val s = local.get(p).targetSlot
    all {
      pc.get(p) == PcLC8,
      if (s != 0 and header.get(b).slotState.get(s) == Trying and slotKey.get(b).get(s) == Some(k)) {
        all {
          header' = header.set(b, updateSlotState(header.get(b), s, Invalid)),
          slotVal' = slotVal.set(b, slotVal.get(b).set(s, None)),
          slotTag' = slotTag.set(b, slotTag.get(b).set(s, None)),
        }
      } else {
        all {
          header' = header,
          slotVal' = slotVal,
          slotTag' = slotTag,
        }
      },
      pc' = pc.set(p, PcLC1),
      unchanged_control,
      slotKey' = slotKey,
      nextTag' = nextTag,
      unchanged_local,
    }
  }

  // LC9 is abort phase 2: pure cleanup — clear slot.Val/Tag, CAS Trying ->
  // Invalid, clear op (the return). The result was already posted to ret at
  // LC7 (the linearization point), so LC9 does NOT touch ret. The slot
  // transitioned Shadow -> Trying at LC7; while Trying it's invisible to
  // L3/I2.5 scans, so clearing the entry pointer here cannot be observed.
  action LC9(p) = {
    val k = keyOfProc(p)
    val b = binOf(k)
    val s = local.get(p).targetSlot
    all {
      pc.get(p) == PcLC9,
      if (s != 0 and header.get(b).slotState.get(s) == Trying and slotKey.get(b).get(s) == Some(k)) {
        all {
          header' = header.set(b, updateSlotState(header.get(b), s, Invalid)),
          slotVal' = slotVal.set(b, slotVal.get(b).set(s, None)),
          slotTag' = slotTag.set(b, slotTag.get(b).set(s, None)),
        }
      } else {
        all {
          header' = header,
          slotVal' = slotVal,
          slotTag' = slotTag,
        }
      },
      pc' = pc.set(p, PcIdle),
      op' = op.set(p, None),
      arg' = arg.set(p, None),
      ret' = ret,
      slotKey' = slotKey,
      nextTag' = nextTag,
      local' = local,
    }
  }

  // ------------------------------------------------------------------------
  // Keep branch predicates centralized and reuse them in both:
  // 1) operation actions (G2/I2/I5/P3/P7/D3), and
  // 2) step-classification wrappers (intermAction/returnAction).
  // This avoids duplicating branching logic in multiple places.

  def g2ReturnGuard(p: PROC): bool = {
    val k = keyOfProc(p)
    val b = binOf(k)
    header.get(b) == local.get(p).h0
  }

  def i2ReturnGuard(p: PROC): bool = {
    val k = keyOfProc(p)
    val b = binOf(k)
    header.get(b) == local.get(p).h0 and visibleSlotsByHeader(local.get(p).h0, b, k).size() != 0
  }

  def i5SuccessGuard(p: PROC): bool = {
    val k = keyOfProc(p)
    val v = valOfProc(p)
    val b = binOf(k)
    val s = local.get(p).targetSlot
    val expectedH = {
      version: local.get(p).h0.version + 1,
      slotState: local.get(p).h0.slotState.set(s, Trying),
    }
    header.get(b) == expectedH and
      slotKey.get(b).get(s) == Some(k) and
      slotVal.get(b).get(s) == Some(v) and
      slotTag.get(b).get(s) != None
  }

  def keyAbsentGuard(p: PROC): bool = {
    val k = keyOfProc(p)
    val b = binOf(k)
    header.get(b) == local.get(p).h0 and local.get(p).targetSlot == 0
  }

  def p7SuccessGuard(p: PROC): bool = {
    val k = keyOfProc(p)
    val b = binOf(k)
    val s = local.get(p).targetSlot
    slotKey.get(b).get(s) == Some(k) and
      slotTag.get(b).get(s) == local.get(p).expectedTag and
      slotTag.get(b).get(s) != None and
      slotVal.get(b).get(s) != None
  }

  def lc2ReturnGuard(p: PROC): bool = {
    val k = keyOfProc(p)
    val b = binOf(k)
    val h = local.get(p).h0
    header.get(b) == h and visibleSlotsByHeader(h, b, k).size() != 0
  }

  def lc5SuccessGuard(p: PROC): bool = {
    val k = keyOfProc(p)
    val b = binOf(k)
    val s = local.get(p).targetSlot
    val expectedH = {
      version: local.get(p).h0.version + 1,
      slotState: local.get(p).h0.slotState.set(s, Trying),
    }
    header.get(b) == expectedH and
      slotKey.get(b).get(s) == Some(k) and
      slotVal.get(b).get(s) == Some(anyVal) and
      slotTag.get(b).get(s) != None
  }

  def lc7SuccessGuard(p: PROC): bool = {
    val k = keyOfProc(p)
    val b = binOf(k)
    val s = local.get(p).targetSlot
    header.get(b) == local.get(p).hLoop and
      local.get(p).hLoop.slotState.get(s) == Shadow
  }

  // LC7 only returns on the save=true commit path; the save=false abort
  // advances to PcLC9 (still intermediate).
  def lc7CommitReturnGuard(p: PROC): bool = {
    lc7SuccessGuard(p) and saveOfProc(p)
  }

  action invokeAction = {
    nondet p = procSet.oneOf()
    invokeLine(p)
  }

  action getIntermAction = {
    nondet p = procSet.oneOf()
    any {
      G1(p),
      all {
        G2(p),
        not(g2ReturnGuard(p)),
      },
    }
  }

  action getReturnAction = {
    nondet p = procSet.oneOf()
    all {
      G2(p),
      g2ReturnGuard(p),
    }
  }

  action insertIntermAction = {
    nondet p = procSet.oneOf()
    any {
      I1(p),
      all {
        I2(p),
        not(i2ReturnGuard(p)),
      },
      I3(p),
      I4(p),
      all {
        I5(p),
        not(i5SuccessGuard(p)),
      },
      I6(p),
    }
  }

  action insertReturnAction = {
    nondet p = procSet.oneOf()
    any {
      all {
        I2(p),
        i2ReturnGuard(p),
      },
      all {
        I5(p),
        i5SuccessGuard(p),
      },
    }
  }

  action putIntermAction = {
    nondet p = procSet.oneOf()
    any {
      P1(p),
      P2(p),
      all {
        P3(p),
        not(keyAbsentGuard(p)),
      },
      P4(p),
      P5(p),
      P6(p),
      all {
        P7(p),
        not(p7SuccessGuard(p)),
      },
    }
  }

  action putReturnAction = {
    nondet p = procSet.oneOf()
    any {
      all {
        P3(p),
        keyAbsentGuard(p),
      },
      all {
        P7(p),
        p7SuccessGuard(p),
      },
    }
  }

  action deleteIntermAction = {
    nondet p = procSet.oneOf()
    any {
      D1(p),
      D2(p),
      all {
        D3(p),
        not(keyAbsentGuard(p)),
      },
      D4(p),
      D5(p),
      D6(p),
      D7(p),
    }
  }

  action deleteReturnAction = {
    nondet p = procSet.oneOf()
    any {
      all {
        D3(p),
        keyAbsentGuard(p),
      },
      D8(p),
    }
  }

  action loadComputeIntermAction = {
    nondet p = procSet.oneOf()
    any {
      LC1(p),
      all {
        LC2(p),
        not(lc2ReturnGuard(p)),
      },
      LC3(p),
      LC4(p),
      LC5(p),
      LC6(p),
      all {
        LC7(p),
        not(lc7CommitReturnGuard(p)),
      },
      LC8(p),
    }
  }

  action loadComputeReturnAction = {
    nondet p = procSet.oneOf()
    any {
      all {
        LC2(p),
        lc2ReturnGuard(p),
      },
      all {
        LC7(p),
        lc7CommitReturnGuard(p),
      },
      LC9(p),
    }
  }

  action intermAction = any {
    getIntermAction,
    insertIntermAction,
    putIntermAction,
    deleteIntermAction,
    loadComputeIntermAction,
  }

  action returnAction = any {
    getReturnAction,
    insertReturnAction,
    putReturnAction,
    deleteReturnAction,
    loadComputeReturnAction,
  }

  action step = any {
    invokeAction,
    intermAction,
    returnAction,
  }

  pure def typeOKPure(st: SpecState): bool = {
    binDomain.forall(b =>
      slotIdx.forall(s =>
        (st.slotVal.get(b).get(s) == None) iff (st.slotTag.get(b).get(s) == None)
      )
    ) and

    binDomain.forall(b => st.header.get(b).version >= 0) and
    st.nextTag >= 1 and

    procSet.forall(p => st.op.get(p) == opFromPC(st.pc.get(p))) and
    procSet.forall(p => slotIdxExt.contains(st.local.get(p).targetSlot)) and
    procSet.forall(p => st.local.get(p).h0.version >= 0 and st.local.get(p).hLoop.version >= 0) and

    procSet.forall(p =>
      if (st.op.get(p) == None)
        st.arg.get(p) == None
      else match st.arg.get(p) {
        | Some(a) => argsOf(st.op.get(p)).contains(a)
        | _ => false
      }
    )
  }

  def typeOK: bool = typeOKPure(currentSpecState)
}
