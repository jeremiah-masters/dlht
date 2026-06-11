module invariants {
  import types.* from "./types"
  import protocol.* from "./protocol"

  // Pure mirrors of protocol's keyOfProc/valOfProc over a snapshot.
  pure def keyOfProcIn(st: SpecState, p: PROC): KEY =
    match st.arg.get(p) { | Some(a) => keyOfArg(a) | _ => anyKey }
  // Pure mirror of protocol's valOfProc over a snapshot; consumed by
  // ownerSlotFilledPure (F2) and retAtLC9Pure (F3d).
  pure def valOfProcIn(st: SpecState, p: PROC): VAL =
    match st.arg.get(p) { | Some(a) => valOfArg(a) | _ => anyVal }

  pure def visibleSlotsWellFormedPure(st: SpecState): bool =
    binDomain.forall(b =>
      slotIdx.forall(s =>
        (
          st.header.get(b).slotState.get(s) == Valid and
            st.slotVal.get(b).get(s) != None
        ).implies(
          st.slotKey.get(b).get(s) != None and st.slotTag.get(b).get(s) != None
        )
      )
    )

  pure def hashConsistencyPure(st: SpecState): bool =
    binDomain.forall(b =>
      slotIdx.forall(s =>
        match st.slotKey.get(b).get(s) {
          | Some(k) => binOf(k) == b
          | _ => true
        }
      )
    )

  // Insert holds Trying through PcI4..PcI6; LoadOrCompute through PcLC4, PcLC5,
  // PcLC8 (unreserve after LC5 failure), and PcLC9 (abort cleanup after LC7 moves Shadow -> Trying).
  pure def ownsTryingIn(st: SpecState, p: PROC, b: BinIdx, s: SlotIdx): bool = {
    (
      (st.op.get(p) == opFromPC(PcI1) and Set(PcI4, PcI5, PcI6).contains(st.pc.get(p))) or
      (st.op.get(p) == opFromPC(PcLC1) and Set(PcLC4, PcLC5, PcLC8, PcLC9).contains(st.pc.get(p)))
    ) and
      binOf(keyOfProcIn(st, p)) == b and
      st.local.get(p).targetSlot == s
  }

  pure def tryingOwnedUniquePure(st: SpecState): bool =
    binDomain.forall(b =>
      slotIdx.forall(s =>
        (st.header.get(b).slotState.get(s) == Trying).implies(
          procSet.exists(p =>
            ownsTryingIn(st, p, b, s) and
              procSet.forall(q => ownsTryingIn(st, q, b, s).implies(q == p))
          )
        )
      )
    )

  // Shadow is held between LC5's CAS and the LC7 commit/abort.
  pure def ownsShadowIn(st: SpecState, p: PROC, b: BinIdx, s: SlotIdx): bool = {
    st.op.get(p) == opFromPC(PcLC1) and
      Set(PcLC6, PcLC7).contains(st.pc.get(p)) and
      binOf(keyOfProcIn(st, p)) == b and
      st.local.get(p).targetSlot == s
  }

  pure def shadowOwnedUniquePure(st: SpecState): bool =
    binDomain.forall(b =>
      slotIdx.forall(s =>
        (st.header.get(b).slotState.get(s) == Shadow).implies(
          procSet.exists(p =>
            ownsShadowIn(st, p, b, s) and
              procSet.forall(q => ownsShadowIn(st, q, b, s).implies(q == p))
          )
        )
      )
    )

  // LC4 writes slotKey before LC5 publishes Shadow, so this is always satisfied
  // for a properly published Shadow slot.
  pure def shadowSlotKeyedPure(st: SpecState): bool =
    binDomain.forall(b =>
      slotIdx.forall(s =>
        (st.header.get(b).slotState.get(s) == Shadow).implies(
          st.slotKey.get(b).get(s) != None
        )
      )
    )

  // Tags are allocated from nextTag, which starts at 1 and only grows, so
  // every live tag is in [1, nextTag). The >= 1 lower bound closes the type:
  // the Int-based induction anchor (induction.qnt) is then provably complete
  // over inv-states without needing Nat-vs-Int reasoning per field.
  pure def tagMonotonicPure(st: SpecState): bool =
    binDomain.forall(b =>
      slotIdx.forall(s =>
        match st.slotTag.get(b).get(s) {
          | Some(t) => t >= 1 and t < st.nextTag
          | _ => true
        }
      )
    ) and
      procSet.forall(p =>
        match st.local.get(p).expectedTag {
          | Some(t) => t >= 1 and t < st.nextTag
          | _ => true
        }
      )

  pure def tagValPairedPure(st: SpecState): bool =
    procSet.forall(p =>
      (
        (st.op.get(p) == opFromPC(PcP1) or st.op.get(p) == opFromPC(PcD1)) and
          Set(PcP6, PcP7, PcD6, PcD7).contains(st.pc.get(p)) and
          st.local.get(p).targetSlot != 0 and
          st.local.get(p).expectedTag != None and
          st.local.get(p).expectedTag == st.slotTag.get(binOf(keyOfProcIn(st, p))).get(st.local.get(p).targetSlot)
      ).implies(
        st.local.get(p).expectedVal == st.slotVal.get(binOf(keyOfProcIn(st, p))).get(st.local.get(p).targetSlot)
      )
    )

  pure def invalidSlotCleanPure(st: SpecState): bool =
    binDomain.forall(b =>
      slotIdx.forall(s =>
        (st.header.get(b).slotState.get(s) == Invalid).implies(
          st.slotVal.get(b).get(s) == None and st.slotTag.get(b).get(s) == None
        )
      )
    )

  // Generalizes visibleKeyUniquePerBin to also forbid two Shadow slots, or a
  // Shadow + Valid pair, sharing a key in the same bin.
  pure def liveKeyUniquePerBinPure(st: SpecState): bool =
    binDomain.forall(b =>
      slotIdx.forall(s1 =>
        slotIdx.forall(s2 =>
          (
            (st.header.get(b).slotState.get(s1) == Valid or st.header.get(b).slotState.get(s1) == Shadow) and
              (st.header.get(b).slotState.get(s2) == Valid or st.header.get(b).slotState.get(s2) == Shadow) and
              st.slotVal.get(b).get(s1) != None and
              st.slotVal.get(b).get(s2) != None and
              st.slotKey.get(b).get(s1) == st.slotKey.get(b).get(s2) and
              st.slotKey.get(b).get(s1) != None
          ).implies(s1 == s2)
        )
      )
    )

  pure def deleteWindowOwnedPure(st: SpecState): bool =
    binDomain.forall(b =>
      slotIdx.forall(s =>
        (
          st.header.get(b).slotState.get(s) == Valid and
            st.slotVal.get(b).get(s) == None and
            st.slotTag.get(b).get(s) == None
        ).implies(
          procSet.exists(p =>
            st.pc.get(p) == PcD8 and
              st.op.get(p) == opFromPC(PcD1) and
              binOf(keyOfProcIn(st, p)) == b and
              st.local.get(p).targetSlot == s
          )
        )
      )
    )

  pure def targetSlotInRangePure(st: SpecState): bool =
    procSet.forall(p =>
      Set(
        PcI4, PcI5, PcI6,
        PcP4, PcP5, PcP6, PcP7,
        PcD4, PcD5, PcD6, PcD7, PcD8,
        PcLC4, PcLC5, PcLC6, PcLC7, PcLC8, PcLC9
      )
        .contains(st.pc.get(p))
        .implies(slotIdx.contains(st.local.get(p).targetSlot))
    )

  // F1: Converse of tryingOwnedUnique/shadowOwnedUnique — the owner's slot IS
  // in the state its PC implies. Why the protocol guarantees it: only the
  // owner transitions its reserved slot. Reserve (I3/LC3) requires Invalid;
  // finalize/unreserve/commit/abort fire only from the owner's PC; D8's AND
  // requires Valid; P7/D7 require a tag match against a Valid-visible slot,
  // and a freshly reserved slot's tag was never visible to any other proc.
  pure def ownerSlotStateConsistentPure(st: SpecState): bool =
    procSet.forall(p => {
      val b = binOf(keyOfProcIn(st, p))
      val s = st.local.get(p).targetSlot
      and {
        (st.op.get(p) == Some(Insert) and Set(PcI4, PcI5, PcI6).contains(st.pc.get(p)))
          .implies(slotIdx.contains(s) and st.header.get(b).slotState.get(s) == Trying),
        (st.op.get(p) == Some(LoadOrCompute) and Set(PcLC4, PcLC5, PcLC8, PcLC9).contains(st.pc.get(p)))
          .implies(slotIdx.contains(s) and st.header.get(b).slotState.get(s) == Trying),
        (st.op.get(p) == Some(LoadOrCompute) and Set(PcLC6, PcLC7).contains(st.pc.get(p)))
          .implies(slotIdx.contains(s) and st.header.get(b).slotState.get(s) == Shadow),
      }
    })

  // F2: After I4/LC4 wrote the slot, the owner's reserved slot holds exactly
  // the owner's key (and for LC the anyVal placeholder, which lc5SuccessGuard
  // requires) plus a tag. No other proc writes a non-Valid slot: P7/D7 cannot
  // match a fresh tag (tags are globally unique via nextTag), reserve requires
  // Invalid. PcI4/PcLC4 are excluded — before the fill, contents are stale.
  pure def ownerSlotFilledPure(st: SpecState): bool =
    procSet.forall(p => {
      val k = keyOfProcIn(st, p)
      val b = binOf(k)
      val s = st.local.get(p).targetSlot
      and {
        (st.op.get(p) == Some(Insert) and Set(PcI5, PcI6).contains(st.pc.get(p)))
          .implies(
            st.slotKey.get(b).get(s) == Some(k) and
            st.slotVal.get(b).get(s) == Some(valOfProcIn(st, p)) and
            st.slotTag.get(b).get(s) != None
          ),
        (st.op.get(p) == Some(LoadOrCompute) and
            Set(PcLC5, PcLC6, PcLC7, PcLC8, PcLC9).contains(st.pc.get(p)))
          .implies(
            st.slotKey.get(b).get(s) == Some(k) and
            st.slotVal.get(b).get(s) == Some(anyVal) and
            st.slotTag.get(b).get(s) != None
          ),
      }
    })

  // F3a: D7 posts ret while op is still Some(Delete); D8 is the unique
  // op!=None && ret!=None window in the whole protocol. Tying ret to
  // expectedVal makes the anchored refinement witness match the abstract
  // Delete's post-linearization configuration. Window contents: the DWCAS
  // (D7) cleared val+tag; nothing can recycle the slot before D8's AND
  // because reserve requires Invalid and the slot is still Valid.
  pure def retAtD8Pure(st: SpecState): bool =
    procSet.forall(p =>
      (st.pc.get(p) == PcD8).implies({
        val k = keyOfProcIn(st, p)
        val b = binOf(k)
        val s = st.local.get(p).targetSlot
        st.ret.get(p) == mkResult(true, st.local.get(p).expectedVal) and
        st.local.get(p).expectedVal != None and
        slotIdx.contains(s) and
        st.header.get(b).slotState.get(s) == Valid and
        st.slotVal.get(b).get(s) == None and
        st.slotTag.get(b).get(s) == None and
        st.slotKey.get(b).get(s) == Some(k)
      })
    )

  // F3d: LC7's save=false branch posts ret (the abort's linearization point)
  // while op stays Some(LoadOrCompute); LC9 is then pure cleanup. PcLC9 is thus
  // a resolved-but-not-returned window (like PcD8). Tie ret to (false, Some(v))
  // so the anchored-refinement witness for a proc at PcLC9 is already resolved —
  // the LC9 return step then preserves the abstract state and refines trivially.
  // The slot window (Trying, key k, val/tag set) is covered by F1/F2.
  pure def retAtLC9Pure(st: SpecState): bool =
    procSet.forall(p =>
      (st.pc.get(p) == PcLC9).implies(
        st.ret.get(p) == mkResult(false, Some(valOfProcIn(st, p)))
      )
    )

  // F3b: outside the two resolved-but-not-returned windows (PcD8 for Delete,
  // PcLC9 for the LoadOrCompute save=false abort) a pending op has no posted ret.
  pure def pendingRetOnlyInRetWindowPure(st: SpecState): bool =
    procSet.forall(p =>
      (st.op.get(p) != None and st.pc.get(p) != PcD8 and st.pc.get(p) != PcLC9)
        .implies(st.ret.get(p) == None)
    )

  // F3c: the D8 finalize window is OWNED — at most one proc per (bin, slot) sits
  // at PcD8, and that proc holds a non-nil expectedTag. Why both facts: to reach
  // PcD8 a proc must pass D7's DWCAS, which requires slotTag(b)(s) == expectedTag
  // AND slotTag != None (so expectedTag != None), and then CLEARS slotVal+slotTag
  // to None. A second delete aiming at the same slot would need its own D7 to
  // fire, but D7 demands slotTag != None and slotVal != None — both already nil
  // after the first D7 — so it diverts to PcD4 instead. Hence the (b,s) D8 window
  // admits exactly one occupant. Consecution needs uniqueness because retAtD8
  // certifies the lone owner's slot as Valid+clean, but says nothing if the
  // anchor forges TWO procs at PcD8 on one slot: the first's D8 clears the slot
  // to Invalid (clearSlotState, version-silent), and the second is left at PcD8
  // over a now-Invalid slot, breaking retAtD8. (This is the delete-side analogue
  // of tryingOwnedUnique / shadowOwnedUnique.)
  pure def d8WindowOwnerUniquePure(st: SpecState): bool =
    procSet.forall(p =>
      (st.pc.get(p) == PcD8).implies(
        st.local.get(p).expectedTag != None and
        procSet.forall(q =>
          (st.pc.get(q) == PcD8 and
            binOf(keyOfProcIn(st, q)) == binOf(keyOfProcIn(st, p)) and
            st.local.get(q).targetSlot == st.local.get(p).targetSlot
          ).implies(q == p)
        )
      )
    )

  // F4: While the reserver's bin is unchanged since its reserve CAS
  // (header == expectedH), its key is absent from the bin's visible AND
  // shadow sets. Why: I2/LC2 verified absence under h0 (the no-visible,
  // no-shadow branch is the only path to I3/LC3); the reserve CAS
  // re-validated header == h0; header-silent writes (P7 value swap, D7
  // clear) can only REMOVE visibility, never create it for a new key; any
  // header-changing step (reserve/finalize/unreserve/D8-AND/LC transitions)
  // breaks the antecedent. This is the linearization argument for I5's
  // finalize, materialized as a state predicate.
  pure def reservedKeyFreshPure(st: SpecState): bool =
    procSet.forall(p => {
      val k = keyOfProcIn(st, p)
      val b = binOf(k)
      val s = st.local.get(p).targetSlot
      val holdsSlot =
        (st.op.get(p) == Some(Insert) and Set(PcI4, PcI5).contains(st.pc.get(p))) or
        (st.op.get(p) == Some(LoadOrCompute) and Set(PcLC4, PcLC5).contains(st.pc.get(p)))
      (holdsSlot and slotIdx.contains(s)).implies({
        val expectedH = {
          version: st.local.get(p).h0.version + 1,
          slotState: st.local.get(p).h0.slotState.set(s, Trying),
        }
        (st.header.get(b) == expectedH).implies(
          visibleSlotsByHeaderFrom(st.header.get(b), st.slotKey, st.slotVal, st.slotTag, b, k).size() == 0 and
          shadowSlotsByHeaderFrom(st.header.get(b), st.slotKey, b, k).size() == 0
        )
      })
    })

  // F4b: The pre-reserve analogue of reservedKeyFresh. At PcI3/PcLC3 the proc
  // has chosen a free targetSlot but has NOT yet executed its reserve CAS, so
  // the live header still equals its h0 snapshot on the success path. I2/LC2
  // route to I3/LC3 only through the innermost branch, which requires
  //   header == h0  AND  not i2/lc2ReturnGuard  (=> visibleSlots(h0)==0, since
  //   header==h0 makes the guard's header check pass)  AND  shadowSlots(h0)==0
  //   AND a free Invalid slot exists.
  // Hence while header is still h0 (the reserve CAS in I3/LC3 will bump the
  // version and break this), the op's key is absent from both the visible and
  // shadow sets of its bin. This is the I3-time link in the same linearization
  // chain that F4 (reservedKeyFresh) continues post-reserve. Consecution needs
  // it because the I3/LC3 reserve step lands the proc at PcI4/PcLC4, where F4's
  // expectedH antecedent (== header after the bump) becomes true and demands
  // exactly this absence — which only an I3-time fact can supply inductively.
  pure def reservedKeyFreshAtReservePure(st: SpecState): bool =
    procSet.forall(p => {
      val k = keyOfProcIn(st, p)
      val b = binOf(k)
      val s = st.local.get(p).targetSlot
      val atReserve =
        (st.op.get(p) == Some(Insert) and st.pc.get(p) == PcI3) or
        (st.op.get(p) == Some(LoadOrCompute) and st.pc.get(p) == PcLC3)
      atReserve.implies(
        (st.header.get(b) == st.local.get(p).h0).implies(
          visibleSlotsByHeaderFrom(st.header.get(b), st.slotKey, st.slotVal, st.slotTag, b, k).size() == 0 and
          shadowSlotsByHeaderFrom(st.header.get(b), st.slotKey, b, k).size() == 0
        )
      )
    })

  // F4c: A post-reserve proc's live bin version is STRICTLY above its h0
  // snapshot. The reserve CAS (I3/LC3) requires header == h0 and writes
  // updateSlotState(h0, ...), i.e. version h0.version + 1; every subsequent
  // header write is updateSlotState (+1) or clearSlotState (D8, +0), so the
  // version never falls below that. Hence at every post-reserve PC
  //   header.version >= h0.version + 1.
  // Why consecution needs it (the cross-proc F4 closure): without it the
  // anchor can build a state where proc q sits post-reserve at PcI5 with
  // h0.version == header.version (its own reserve bump "forgotten"), and
  // q.expectedH (= h0 with version+1) is therefore ONE unrelated version bump
  // away from matching the live header. Another proc's I6/I5/D8 step supplies
  // that bump, the F4 antecedent (header == expectedH) springs true, and any
  // slot still Valid-visible for q's key violates F4. This bound makes q's
  // reserve bump first-class, so such an anchor state is rejected outright:
  // header.version >= h0.version + 1 already excludes the "armed by a foreign
  // bump" configuration. (Strictness — +1 not +0 — is the whole point;
  // versionMonotonic only gives h0.version <= header.version.)
  pure val postReserveInsertPCs: Set[PC] = Set(PcI4, PcI5, PcI6)
  pure val postReserveLCPCs: Set[PC] = Set(PcLC4, PcLC5, PcLC6, PcLC7, PcLC8, PcLC9)

  pure def reserveBumpedVersionPure(st: SpecState): bool =
    procSet.forall(p => {
      val b = binOf(keyOfProcIn(st, p))
      and {
        (st.op.get(p) == Some(Insert) and postReserveInsertPCs.contains(st.pc.get(p)))
          .implies(st.header.get(b).version >= st.local.get(p).h0.version + 1),
        (st.op.get(p) == Some(LoadOrCompute) and postReserveLCPCs.contains(st.pc.get(p)))
          .implies(st.header.get(b).version >= st.local.get(p).h0.version + 1),
      }
    })

  // F4d: the seqlock agreement for the reserver's COMPUTED snapshot expectedH
  // (= h0 with the reserved slot flipped to Trying, version h0.version+1) — the
  // expectedH analogue of snapStateAgrees (F8, which only covers the STORED
  // h0/hLoop). At I4/I5/LC4/LC5 the reserve CAS (I3/LC3) set header EXACTLY to
  // expectedH; while the live version is still expectedH.version, the only
  // header writes that could have happened since are version-silent D8 clears
  // (Valid -> Invalid) — anything else bumps the version off expectedH.version.
  // Hence header agrees with expectedH up to slots that went Valid -> Invalid:
  //   header.slotState(t) == expectedH.slotState(t)
  //     OR (expectedH.slotState(t) == Valid AND header.slotState(t) == Invalid).
  // Why F4 (reservedKeyFresh) needs this to be INDUCTIVE: F4 fires only on the
  // strict header == expectedH, but a version-silent D8 by another proc can
  // EDGE the live header INTO equality with expectedH from a state where they
  // differed (so F4 was vacuous the step before). The anchor exploited exactly
  // that: a forged state with header.slotState(reserved-by-q's-sibling) == Valid
  // while expectedH had it Invalid, then a D8 clear made them equal and surfaced
  // a stale visible key. That forged state is impossible — Invalid (in expectedH)
  // to Valid (live) at one fixed version needs a reserve+commit (two bumps) — and
  // this conjunct rejects it: expectedH==Invalid with live==Valid is not a
  // permitted clear-difference. With F4d, every state reachable by a version-
  // silent step into header==expectedH already had expectedH-agreement, so F4's
  // antecedent only becomes true on genuinely-current snapshots.
  pure def reserveHeaderAgreesPure(st: SpecState): bool =
    procSet.forall(p => {
      val b = binOf(keyOfProcIn(st, p))
      val s = st.local.get(p).targetSlot
      val holdsSlot =
        (st.op.get(p) == Some(Insert) and Set(PcI4, PcI5).contains(st.pc.get(p))) or
        (st.op.get(p) == Some(LoadOrCompute) and Set(PcLC4, PcLC5).contains(st.pc.get(p)))
      (holdsSlot and slotIdx.contains(s)).implies({
        val expectedH = {
          version: st.local.get(p).h0.version + 1,
          slotState: st.local.get(p).h0.slotState.set(s, Trying),
        }
        (st.header.get(b).version == expectedH.version).implies(
          slotIdx.forall(t =>
            st.header.get(b).slotState.get(t) == expectedH.slotState.get(t) or
            (expectedH.slotState.get(t) == Valid and st.header.get(b).slotState.get(t) == Invalid)
          )
        )
      })
    })

  // F6: At the Put/Delete commit window (P6/P7, D6/D7) the proc's OWN hLoop
  // snapshot records its target slot as Valid. P5/D5 advance to P6/D6 only when
  //   retry == false  ==  (hLoop.slotState(s) == Valid AND slotKey(b)(s)==k AND
  //                        slotVal(b)(s) != None);
  // hLoop is local and is not rewritten between P5->P6->P7 (resp. D5->D6->D7),
  // so hLoop.slotState(s) == Valid persists across the whole window. (Note this
  // is a fact about the LOCAL snapshot, independent of concurrent header
  // motion; the live-header agreement is re-checked by P6/D6's header==hLoop.)
  // Consecution needs it because retAtD8 / tagValPaired reconstruct the commit
  // from the assumption that the target slot was a Valid, key-matching entry;
  // an adversarial hLoop with slotState(s) == Trying (a slot another proc holds
  // as a reservation) would let D7 'commit' a delete over a foreign Trying slot
  // and land at PcD8 with the slot non-Valid, breaking retAtD8.
  pure def loopSlotValidAtCommitPure(st: SpecState): bool =
    procSet.forall(p => {
      val s = st.local.get(p).targetSlot
      and {
        (st.op.get(p) == Some(Put) and Set(PcP6, PcP7).contains(st.pc.get(p)))
          .implies(slotIdx.contains(s) and st.local.get(p).hLoop.slotState.get(s) == Valid),
        (st.op.get(p) == Some(Delete) and Set(PcD6, PcD7).contains(st.pc.get(p)))
          .implies(slotIdx.contains(s) and st.local.get(p).hLoop.slotState.get(s) == Valid),
      }
    })

  // F7: The DWCAS tag-identity argument, as a state predicate. At the FINAL
  // commit line (PcP7 / PcD7 — strictly AFTER the P6/D6 header re-check), IF the
  // live slot still carries the proc's captured expectedTag (non-nil), THEN the
  // slot is Valid. Why: a tag is globally unique (nextTag++, never reused), and
  // EVERY slotTag write either clears it to None (D7 — the only header-silent
  // one; I6/LC8/LC9 go through updateSlotState, which bumps the version) or
  // overwrites it with a fresh tag (I4/LC4/P7 header-silently, the LC7 commit
  // with a version bump). P6/D6 succeed only when
  // header == hLoop, and F6 gives hLoop.slotState(s) == Valid, so the slot was
  // Valid the instant the proc entered P7/D7; any later departure from Valid
  // rewrites/clears the tag, so a slot STILL bearing the captured tag is still
  // that same Valid entry. A Trying/Shadow occupant — necessarily another proc's
  // reservation — carries a fresh, different tag.
  //   IMPORTANT — why P6/D6 are EXCLUDED: P5/D5's retry test reads the LOCAL,
  // possibly-stale hLoop.slotState (Valid) but the LIVE slotKey/slotVal/slotTag.
  // So a proc can legitimately capture expectedTag at P5/D5 from a slot that is
  // LIVE-Trying (a concurrent Insert's freshly filled reservation) while its
  // hLoop still shows the old Valid occupant. That proc then sits at PcP6/PcD6
  // with expectedTag == a Trying slot's tag — a REACHABLE state — until P6/D6's
  // header==hLoop check fails and bounces it back to P4/D4. Covering P6/D6 here
  // would falsely ban that benign intermediate (simulator-confirmed reachable).
  // The commit only fires from P7/D7, and the P6/D6 gate guarantees the slot was
  // Valid on entry, so restricting F7 to P7/D7 is exactly the sound boundary.
  // Consecution needs it because D7's/P7's success branch fires on the bare tag
  // match and D7 then lands at PcD8 (retAtD8 demands Valid); tag-uniqueness is
  // the only bridge from 'tag matched' to 'slot Valid'.
  pure def commitTagImpliesValidPure(st: SpecState): bool =
    procSet.forall(p => {
      val b = binOf(keyOfProcIn(st, p))
      val s = st.local.get(p).targetSlot
      val atCommit =
        (st.op.get(p) == Some(Put) and st.pc.get(p) == PcP7) or
        (st.op.get(p) == Some(Delete) and st.pc.get(p) == PcD7)
      (atCommit and slotIdx.contains(s) and
        st.local.get(p).expectedTag != None and
        st.slotTag.get(b).get(s) == st.local.get(p).expectedTag
      ).implies(st.header.get(b).slotState.get(s) == Valid)
    })

  // F5: Local header snapshots never run ahead of the live header. Versions
  // only grow (updateSlotState bumps, clearSlotState preserves), and
  // h0/hLoop were loaded from the op's own bin after the op started; arg
  // (hence the bin) only changes at invoke, which routes through the
  // excluded first-line PCs where the snapshot is refreshed before use.
  pure val h0LivePCs: Set[PC] = Set(
    PcG2, PcI2, PcI3, PcI4, PcI5, PcI6,
    PcP2, PcP3, PcP4, PcP5, PcP6, PcP7,
    PcD2, PcD3, PcD4, PcD5, PcD6, PcD7, PcD8,
    PcLC2, PcLC3, PcLC4, PcLC5, PcLC6, PcLC7, PcLC8, PcLC9
  )
  pure val hLoopLivePCs: Set[PC] = Set(PcP5, PcP6, PcP7, PcD5, PcD6, PcD7, PcLC7)

  pure def versionMonotonicPure(st: SpecState): bool =
    procSet.forall(p => {
      val b = binOf(keyOfProcIn(st, p))
      and {
        h0LivePCs.contains(st.pc.get(p))
          .implies(st.local.get(p).h0.version <= st.header.get(b).version),
        hLoopLivePCs.contains(st.pc.get(p))
          .implies(st.local.get(p).hLoop.version <= st.header.get(b).version),
      }
    })

  // F2b: The post-reserve, PRE-fill window. At PcI4 / PcLC4 the proc has
  // executed its reserve CAS (slot Invalid -> Trying) but NOT yet its fill
  // (I4 / LC4 write key+val+tag); so its target slot still carries
  //   slotVal == None  and  slotTag == None.
  // Why: I3/LC3 reserve requires header.slotState(s) == Invalid, and
  // invalidSlotClean gives Invalid => slotVal == None and slotTag == None; the
  // reserve writes only the header (slotState), never the entry, so val/tag are
  // still None when the proc reaches PcI4/PcLC4 (the very next fill is what
  // populates them). slotKey is intentionally NOT constrained — frees (I6, D7,
  // LC8, LC9) clear val+tag but leave a stale key behind. This is the empty-slot
  // sibling of F2 (ownerSlotFilled, which starts at PcI5/PcLC5 AFTER the fill).
  // Consecution needs it because P5/D5's retry test reads hLoop.slotState (which
  // can be a stale Valid) together with the LIVE slotKey/slotVal; an anchor that
  // leaves a fresh PcI4 reservation carrying a previous occupant's val+tag lets
  // a concurrent Put 'see' a populated slot, capture its tag, and trip
  // commitTagImpliesValid — even though that reservation is empty in every real
  // run. Pinning val/tag == None at PcI4/PcLC4 removes that forged occupant.
  pure def freshReservationEmptyPure(st: SpecState): bool =
    procSet.forall(p => {
      val b = binOf(keyOfProcIn(st, p))
      val s = st.local.get(p).targetSlot
      val atFill =
        (st.op.get(p) == Some(Insert) and st.pc.get(p) == PcI4) or
        (st.op.get(p) == Some(LoadOrCompute) and st.pc.get(p) == PcLC4)
      (atFill and slotIdx.contains(s)).implies(
        st.slotVal.get(b).get(s) == None and st.slotTag.get(b).get(s) == None
      )
    })

  // F8: The seqlock agreement. A loaded snapshot (h0/hLoop, from the proc's own
  // bin — same PC sets as F5) whose version EQUALS the live header version
  // differs from the live slotState only by slots that went Valid -> Invalid.
  // Why exactly that exception and nothing more: every header write is either
  //   updateSlotState (version + 1)  or  clearSlotState (D8: version + 0, and it
  //   ONLY moves a slot Valid -> Invalid).
  // The snapshot was taken from this bin's header at version X. Versions never
  // decrease and never repeat, so once the live version leaves X it can never
  // return to X; therefore at any state where live.version == snap.version == X,
  // NO updateSlotState has occurred since the snapshot — only D8 clears, each of
  // which flips one Valid to Invalid. Hence per slot:
  //   snap.slotState(s) == live.slotState(s)
  //     OR (snap.slotState(s) == Valid AND live.slotState(s) == Invalid).
  // Consecution needs it because P5/D5 advance on hLoop.slotState(s)==Valid
  // (F6) WITHOUT yet re-checking the live header (that is P6/D6's job); the
  // anchor otherwise forges a same-version hLoop showing Valid while the live
  // slot is Trying/Shadow (another proc's reservation), so P5/D5 capture
  // expectedTag from a foreign live slot and the next step trips
  // commitTagImpliesValid. This is the version half of the seqlock that F7's
  // tag half complements: equal versions pin the state up to benign deletes.
  pure def snapAgreesUpToClear(snap: Header, live: Header): bool =
    slotIdx.forall(s =>
      snap.slotState.get(s) == live.slotState.get(s) or
      (snap.slotState.get(s) == Valid and live.slotState.get(s) == Invalid)
    )

  pure def snapStateAgreesPure(st: SpecState): bool =
    procSet.forall(p => {
      val b = binOf(keyOfProcIn(st, p))
      and {
        (h0LivePCs.contains(st.pc.get(p)) and
          st.local.get(p).h0.version == st.header.get(b).version)
          .implies(snapAgreesUpToClear(st.local.get(p).h0, st.header.get(b))),
        (hLoopLivePCs.contains(st.pc.get(p)) and
          st.local.get(p).hLoop.version == st.header.get(b).version)
          .implies(snapAgreesUpToClear(st.local.get(p).hLoop, st.header.get(b))),
      }
    })

  // F9: For the refinement at the P3/D3 absent-return path. keyAbsentGuard
  // (P3/D3) fires when header == h0 AND targetSlot == 0, and returns
  // (false, None) "key absent". For that to refine the abstract op, k must be
  // genuinely absent from the bin's VISIBLE set in this anchored state. The
  // protocol guarantees it: targetSlot was computed at P2/D2 as
  //   slotForKeyByHeader(h0, b, k) = minOr0(visibleSlotsByHeader(h0, b, k)),
  // so targetSlot == 0 means NO slot was visible for k under the snapshot h0.
  // While header == h0 still holds (the seqlock has not moved since the
  // snapshot), no slot can BECOME visible-for-k: a slot becomes Valid-visible
  // only via finalize (Invalid/Trying -> Valid, I5/LC7-commit) or a P7 value
  // swap onto an already-Valid slot, and finalize bumps the version (breaking
  // header == h0) while a P7 swap onto a slot that was non-visible under h0
  // would require that slot to be Valid under h0 with a different key — but
  // then it isn't k's visible slot either. So at P3/D3 with header == h0 AND
  // targetSlot == 0, k is absent from the live visible set, matching the
  // abstract absent-key return. The adversarial anchor fabricated targetSlot==0
  // while h0 showed k visible (slot 3 Valid, key k1) — impossible in a real
  // execution, since slotForKeyByHeader(h0) would then have returned 3, not 0.
  pure def absentReturnConsistentPure(st: SpecState): bool =
    procSet.forall(p => {
      val k = keyOfProcIn(st, p)
      val b = binOf(k)
      (Set(PcP3, PcD3).contains(st.pc.get(p)) and
        st.header.get(b) == st.local.get(p).h0 and
        st.local.get(p).targetSlot == 0
      ).implies(
        visibleSlotsByHeaderFrom(st.header.get(b), st.slotKey, st.slotVal, st.slotTag, b, k).size() == 0
      )
    })

  // ======================= CONTRACT COROLLARIES ===========================
  // LoadOrCompute CONTRACT properties, machine-checked as
  //     inv ⟹ corollary
  // by a 0-step Apalache run from the induction anchor (indInit guards every
  // candidate with invPure, so `--max-steps=0` evaluates the corollary over
  // ALL inv-states, not just reachable ones):
  //     quint verify verify/induction_2proc.qnt \
  //       --invariant=f_contractAtMostOneComputePerKey --max-steps=0
  // (wired in verify.sh stage 4, DLHT_VERIFY_2PROC tier; 2-proc is the right
  // config — at 1-proc the at-most-once property is trivially true).
  // These are deliberately NOT conjuncts of invPure: each is already IMPLIED
  // by inv, so conjoining them would change the induction anchor and force a
  // full re-verification of every consecution + anchored-refinement run for
  // zero added strength. Keep them out of invPure.

  // Contract clause: AT-MOST-ONCE fn EXECUTION per key. The fn-execution
  // window is PcLC6/PcLC7 — from LC5's Shadow publish to LC7's commit/abort
  // resolution; the modeled fn runs exactly once inside it. At most one proc
  // per key occupies that window at any instant, so fn never runs twice
  // concurrently for one key (a SECOND compute may legitimately run after an
  // abort resolves — see scenario lc_abort_handoff_second_compute_runs — and
  // may overlap the aborter's PcLC9 cleanup, which is outside the window:
  // invocation overlap is not fn overlap, scenario
  // lc9_window_second_compute_overlaps).
  // WHY a corollary check, not an invPure conjunct: implied by inv —
  // ownerSlotStateConsistent + ownerSlotFilled put each window proc on a
  // Shadow slot keyed (and placeholder-val-filled) with its own key, so two
  // same-key window procs would mean either two val-bearing Shadow slots
  // sharing a key (banned by liveKeyUniquePerBin) or two owners of one Shadow
  // slot (banned by shadowOwnedUnique).
  pure def contractAtMostOneComputePerKeyPure(st: SpecState): bool =
    procSet.forall(p =>
      procSet.forall(q =>
        (
          st.op.get(p) == Some(LoadOrCompute) and
            Set(PcLC6, PcLC7).contains(st.pc.get(p)) and
            st.op.get(q) == Some(LoadOrCompute) and
            Set(PcLC6, PcLC7).contains(st.pc.get(q)) and
            keyOfProcIn(st, p) == keyOfProcIn(st, q)
        ).implies(p == q)
      )
    )

  // Contract clause: HOLDER EXCLUSIVITY (guaranteed commit). While a proc is
  // in the fn window (PcLC6/PcLC7) its claim is intact — it still holds its
  // Shadow slot (in range, state Shadow, keyed with its k) — and k is still
  // UNCOMMITTED: the bin's visible set for k is empty (same idiom as
  // absentReturnConsistent). Nothing can publish k behind the holder's back,
  // so the holder's LC7 resolution is the unique next visibility event for k
  // and a save=true commit cannot collide with an existing visible entry.
  // WHY a corollary check, not an invPure conjunct: implied by inv —
  // ownerSlotStateConsistent/ownerSlotFilled give the Shadow + keyed +
  // val-bearing slot; any visible slot for k would be a Valid, val-bearing,
  // k-keyed slot coexisting with it, which liveKeyUniquePerBin forbids (the
  // two slots would have to coincide, but Valid != Shadow). Adding it to
  // invPure would change the anchor and force full re-verification for no
  // strengthening.
  pure def contractHolderExclusivePure(st: SpecState): bool =
    procSet.forall(p => {
      val k = keyOfProcIn(st, p)
      val b = binOf(k)
      val s = st.local.get(p).targetSlot
      val inFnWindow =
        st.op.get(p) == Some(LoadOrCompute) and
          Set(PcLC6, PcLC7).contains(st.pc.get(p))
      inFnWindow.implies(
        slotIdx.contains(s) and
        st.header.get(b).slotState.get(s) == Shadow and
        st.slotKey.get(b).get(s) == Some(k) and
        visibleSlotsByHeaderFrom(st.header.get(b), st.slotKey, st.slotVal, st.slotTag, b, k).size() == 0
      )
    })

  // Live wrappers: each family is individually checkable via
  // --invariant=<name> for counterexample localization (inv-group splitting).
  def visibleSlotsWellFormed = visibleSlotsWellFormedPure(currentSpecState)
  def hashConsistency = hashConsistencyPure(currentSpecState)
  def tryingOwnedUnique = tryingOwnedUniquePure(currentSpecState)
  def shadowOwnedUnique = shadowOwnedUniquePure(currentSpecState)
  def shadowSlotKeyed = shadowSlotKeyedPure(currentSpecState)
  def tagMonotonic = tagMonotonicPure(currentSpecState)
  def tagValPaired = tagValPairedPure(currentSpecState)
  def invalidSlotClean = invalidSlotCleanPure(currentSpecState)
  def liveKeyUniquePerBin = liveKeyUniquePerBinPure(currentSpecState)
  def deleteWindowOwned = deleteWindowOwnedPure(currentSpecState)
  def targetSlotInRange = targetSlotInRangePure(currentSpecState)
  def ownerSlotStateConsistent = ownerSlotStateConsistentPure(currentSpecState)
  def ownerSlotFilled = ownerSlotFilledPure(currentSpecState)
  def retAtD8 = retAtD8Pure(currentSpecState)
  def retAtLC9 = retAtLC9Pure(currentSpecState)
  def pendingRetOnlyInRetWindow = pendingRetOnlyInRetWindowPure(currentSpecState)
  def d8WindowOwnerUnique = d8WindowOwnerUniquePure(currentSpecState)
  def reservedKeyFresh = reservedKeyFreshPure(currentSpecState)
  def reservedKeyFreshAtReserve = reservedKeyFreshAtReservePure(currentSpecState)
  def reserveBumpedVersion = reserveBumpedVersionPure(currentSpecState)
  def reserveHeaderAgrees = reserveHeaderAgreesPure(currentSpecState)
  def loopSlotValidAtCommit = loopSlotValidAtCommitPure(currentSpecState)
  def commitTagImpliesValid = commitTagImpliesValidPure(currentSpecState)
  def versionMonotonic = versionMonotonicPure(currentSpecState)
  def snapStateAgrees = snapStateAgreesPure(currentSpecState)
  def freshReservationEmpty = freshReservationEmptyPure(currentSpecState)
  def absentReturnConsistent = absentReturnConsistentPure(currentSpecState)

  // Contract-corollary wrappers (checked standalone as inv ⟹ corollary in
  // verify.sh stage 4; NOT part of inv/invPure — see the section above).
  def contractAtMostOneComputePerKey = contractAtMostOneComputePerKeyPure(currentSpecState)
  def contractHolderExclusive = contractHolderExclusivePure(currentSpecState)

  // The combined inductive candidate — the anchor guard for induction.qnt.
  // Note: typeOKPure is defined in protocol.qnt (it owns the vars and the
  // SpecState type); every other family is defined above in this module.
  pure def invPure(st: SpecState): bool =
    typeOKPure(st) and
      visibleSlotsWellFormedPure(st) and
      hashConsistencyPure(st) and
      tryingOwnedUniquePure(st) and
      shadowOwnedUniquePure(st) and
      shadowSlotKeyedPure(st) and
      tagMonotonicPure(st) and
      tagValPairedPure(st) and
      invalidSlotCleanPure(st) and
      liveKeyUniquePerBinPure(st) and
      deleteWindowOwnedPure(st) and
      targetSlotInRangePure(st) and
      ownerSlotStateConsistentPure(st) and
      ownerSlotFilledPure(st) and
      retAtD8Pure(st) and
      retAtLC9Pure(st) and
      pendingRetOnlyInRetWindowPure(st) and
      d8WindowOwnerUniquePure(st) and
      reservedKeyFreshPure(st) and
      reservedKeyFreshAtReservePure(st) and
      reserveBumpedVersionPure(st) and
      reserveHeaderAgreesPure(st) and
      loopSlotValidAtCommitPure(st) and
      commitTagImpliesValidPure(st) and
      versionMonotonicPure(st) and
      snapStateAgreesPure(st) and
      freshReservationEmptyPure(st) and
      absentReturnConsistentPure(st)

  def inv: bool = invPure(currentSpecState)

  // Re-exports for verify modules (Quint parameterized star-imports
  // do not transitively export names from imported modules)
  action invariantsInit = init
  action invariantsStep = step
  def invariantsTypeOK = typeOK
}
