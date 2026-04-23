module invariants {
  import types.* from "./types"
  import protocol.* from "./protocol"

  def visibleSlotsWellFormed: bool =
    binDomain.forall(b =>
      slotIdx.forall(s =>
        (
          header.get(b).slotState.get(s) == Valid and
            slotVal.get(b).get(s) != None
        ).implies(
          slotKey.get(b).get(s) != None and slotTag.get(b).get(s) != None
        )
      )
    )

  def hashConsistency: bool =
    binDomain.forall(b =>
      slotIdx.forall(s =>
        match slotKey.get(b).get(s) {
          | Some(k) => binOf(k) == b
          | _ => true
        }
      )
    )

  def tryingOwnedUnique: bool =
    binDomain.forall(b =>
      slotIdx.forall(s =>
        (header.get(b).slotState.get(s) == Trying).implies(
          procSet.exists(p =>
            op.get(p) == opFromPC(PcI1) and
              Set(PcI4, PcI5, PcI6).contains(pc.get(p)) and
              binOf(keyOfProc(p)) == b and
              local.get(p).targetSlot == s and
              procSet.forall(q =>
                (
                  op.get(q) == opFromPC(PcI1) and
                    Set(PcI4, PcI5, PcI6).contains(pc.get(q)) and
                    binOf(keyOfProc(q)) == b and
                    local.get(q).targetSlot == s
                ).implies(q == p)
              )
          )
        )
      )
    )

  def tagMonotonic: bool =
    binDomain.forall(b =>
      slotIdx.forall(s =>
        match slotTag.get(b).get(s) {
          | Some(t) => t < nextTag
          | _ => true
        }
      )
    ) and
      procSet.forall(p =>
        match local.get(p).expectedTag {
          | Some(t) => t < nextTag
          | _ => true
        }
      )

  def tagValPaired: bool =
    procSet.forall(p =>
      (
        (op.get(p) == opFromPC(PcP1) or op.get(p) == opFromPC(PcD1)) and
          Set(PcP6, PcP7, PcD6, PcD7).contains(pc.get(p)) and
          local.get(p).targetSlot != 0 and
          local.get(p).expectedTag != None and
          local.get(p).expectedTag == slotTag.get(binOf(keyOfProc(p))).get(local.get(p).targetSlot)
      ).implies(
        local.get(p).expectedVal == slotVal.get(binOf(keyOfProc(p))).get(local.get(p).targetSlot)
      )
    )

  def invalidSlotClean: bool =
    binDomain.forall(b =>
      slotIdx.forall(s =>
        (header.get(b).slotState.get(s) == Invalid).implies(
          slotVal.get(b).get(s) == None and slotTag.get(b).get(s) == None
        )
      )
    )

  def visibleKeyUniquePerBin: bool =
    binDomain.forall(b =>
      slotIdx.forall(s1 =>
        slotIdx.forall(s2 =>
          (
            header.get(b).slotState.get(s1) == Valid and
              header.get(b).slotState.get(s2) == Valid and
              slotVal.get(b).get(s1) != None and
              slotVal.get(b).get(s2) != None and
              slotKey.get(b).get(s1) == slotKey.get(b).get(s2) and
              slotKey.get(b).get(s1) != None
          ).implies(s1 == s2)
        )
      )
    )

  def deleteWindowOwned: bool =
    binDomain.forall(b =>
      slotIdx.forall(s =>
        (
          header.get(b).slotState.get(s) == Valid and
            slotVal.get(b).get(s) == None and
            slotTag.get(b).get(s) == None
        ).implies(
          procSet.exists(p =>
            pc.get(p) == PcD8 and
              op.get(p) == opFromPC(PcD1) and
              binOf(keyOfProc(p)) == b and
              local.get(p).targetSlot == s
          )
        )
      )
    )

  def targetSlotInRange: bool =
    procSet.forall(p =>
      Set(PcI4, PcI5, PcI6, PcP4, PcP5, PcP6, PcP7, PcD4, PcD5, PcD6, PcD7, PcD8)
        .contains(pc.get(p))
        .implies(slotIdx.contains(local.get(p).targetSlot))
    )

  def inv: bool =
    typeOK and
      visibleSlotsWellFormed and
      hashConsistency and
      tryingOwnedUnique and
      tagMonotonic and
      tagValPaired and
      invalidSlotClean and
      visibleKeyUniquePerBin and
      deleteWindowOwned and
      targetSlotInRange

  // Re-exports for verify modules (Quint parameterized star-imports
  // do not transitively export names from imported modules)
  action invariantsInit = init
  action invariantsStep = step
  def invariantsTypeOK = typeOK
}
