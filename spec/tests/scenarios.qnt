module scenarios {
  import types(
    procSet = Set("p1", "p2"),
    keyDomain = Set("k1", "k2"),
    valDomain = Set("v1", "v2"),
  ).* from "../types"

  import checked(
    procSet = Set("p1", "p2"),
    keyDomain = Set("k1", "k2"),
    valDomain = Set("v1", "v2"),
    numBins = 1,
    slotsPerBin = 3,
    hash = Map("k1" -> 1, "k2" -> 1),
  ) as protocol from "../checked"

  def retIs(p, okExpected, valExpected) = {
    match protocol::ret.get(p) {
      | Some(r) => r.ok == okExpected and r.resVal == valExpected
      | _ => false
    }
  }

  action invokeGet(p, k) = all {
    protocol::pc.get(p) == protocol::PcIdle,
    protocol::pc' = protocol::pc.set(p, protocol::PcG1),
    protocol::op' = protocol::op.set(p, Some(Get)),
    protocol::arg' = protocol::arg.set(p, Some(KeyArg(k))),
    protocol::ret' = protocol::ret.set(p, None),
    protocol::unchanged_shared,
    protocol::unchanged_local,
  }

  action invokeInsert(p, k, v) = all {
    protocol::pc.get(p) == protocol::PcIdle,
    protocol::pc' = protocol::pc.set(p, protocol::PcI1),
    protocol::op' = protocol::op.set(p, Some(Insert)),
    protocol::arg' = protocol::arg.set(p, Some(KeyValArg((k, v)))),
    protocol::ret' = protocol::ret.set(p, None),
    protocol::unchanged_shared,
    protocol::unchanged_local,
  }

  action invokePut(p, k, v) = all {
    protocol::pc.get(p) == protocol::PcIdle,
    protocol::pc' = protocol::pc.set(p, protocol::PcP1),
    protocol::op' = protocol::op.set(p, Some(Put)),
    protocol::arg' = protocol::arg.set(p, Some(KeyValArg((k, v)))),
    protocol::ret' = protocol::ret.set(p, None),
    protocol::unchanged_shared,
    protocol::unchanged_local,
  }

  action invokeDelete(p, k) = all {
    protocol::pc.get(p) == protocol::PcIdle,
    protocol::pc' = protocol::pc.set(p, protocol::PcD1),
    protocol::op' = protocol::op.set(p, Some(Delete)),
    protocol::arg' = protocol::arg.set(p, Some(KeyArg(k))),
    protocol::ret' = protocol::ret.set(p, None),
    protocol::unchanged_shared,
    protocol::unchanged_local,
  }

  action invokeLoadOrCompute(p, k, v, save) = all {
    protocol::pc.get(p) == protocol::PcIdle,
    protocol::pc' = protocol::pc.set(p, protocol::PcLC1),
    protocol::op' = protocol::op.set(p, Some(LoadOrCompute)),
    protocol::arg' = protocol::arg.set(p, Some(LoadComputeArg((k, v, save)))),
    protocol::ret' = protocol::ret.set(p, None),
    protocol::unchanged_shared,
    protocol::unchanged_local,
  }

  action insertSuccess(p, k, v) =
    invokeInsert(p, k, v)
      .then(protocol::I1(p))
      .then(protocol::I2(p))
      .then(protocol::I3(p))
      .then(protocol::I4(p))
      .then(protocol::I5(p))

  action deleteToD7(p, k) =
    invokeDelete(p, k)
      .then(protocol::D1(p))
      .then(protocol::D2(p))
      .then(protocol::D3(p))
      .then(protocol::D4(p))
      .then(protocol::D5(p))
      .then(protocol::D6(p))
      .then(protocol::D7(p))

  action putToP6(p, k, v) =
    invokePut(p, k, v)
      .then(protocol::P1(p))
      .then(protocol::P2(p))
      .then(protocol::P3(p))
      .then(protocol::P4(p))
      .then(protocol::P5(p))
      .then(protocol::P6(p))

  run single_process_get_empty =
    protocol::init
      .then(invokeGet("p1", "k1"))
      .then(protocol::G1("p1"))
      .then(protocol::G2("p1"))
      .expect(retIs("p1", false, None))

  run insert_then_get =
    protocol::init
      .then(insertSuccess("p1", "k1", "v1"))
      .expect(retIs("p1", true, None))
      .then(invokeGet("p1", "k1"))
      .then(protocol::G1("p1"))
      .then(protocol::G2("p1"))
      .expect(retIs("p1", true, Some("v1")))

  run insert_duplicate_fails =
    protocol::init
      .then(insertSuccess("p1", "k1", "v1"))
      .then(invokeInsert("p1", "k1", "v2"))
      .then(protocol::I1("p1"))
      .then(protocol::I2("p1"))
      .expect(retIs("p1", false, Some("v1")))

  run put_existing_succeeds =
    protocol::init
      .then(insertSuccess("p1", "k1", "v1"))
      .then(invokePut("p1", "k1", "v2"))
      .then(protocol::P1("p1"))
      .then(protocol::P2("p1"))
      .then(protocol::P3("p1"))
      .then(protocol::P4("p1"))
      .then(protocol::P5("p1"))
      .then(protocol::P6("p1"))
      .then(protocol::P7("p1"))
      .expect(retIs("p1", true, Some("v1")))
      .expect(protocol::readVisibleByHeader(protocol::header.get(1), 1, "k1") == Some("v2"))

  run put_absent_fails =
    protocol::init
      .then(invokePut("p1", "k1", "v1"))
      .then(protocol::P1("p1"))
      .then(protocol::P2("p1"))
      .then(protocol::P3("p1"))
      .expect(retIs("p1", false, None))

  run delete_existing_succeeds =
    protocol::init
      .then(insertSuccess("p1", "k1", "v1"))
      .then(deleteToD7("p1", "k1"))
      .expect(retIs("p1", true, Some("v1")))
      .then(protocol::D8("p1"))
      .expect(protocol::op.get("p1") == None)
      .expect(protocol::readVisibleByHeader(protocol::header.get(1), 1, "k1") == None)

  run delete_window_cleanup =
    protocol::init
      .then(insertSuccess("p1", "k1", "v1"))
      .then(deleteToD7("p1", "k1"))
      .expect(
        protocol::pc.get("p1") == protocol::PcD8 and
          protocol::op.get("p1") == Some(Delete) and
          protocol::header.get(1).slotState.get(protocol::local.get("p1").targetSlot) == protocol::Valid and
          protocol::slotVal.get(1).get(protocol::local.get("p1").targetSlot) == None and
          protocol::slotTag.get(1).get(protocol::local.get("p1").targetSlot) == None
      )
      .then(protocol::D8("p1"))
      .expect(protocol::header.get(1).slotState.get(protocol::local.get("p1").targetSlot) == protocol::Invalid)

  run concurrent_insert_contention =
    protocol::init
      .then(invokeInsert("p1", "k1", "v1"))
      .then(invokeInsert("p2", "k1", "v2"))
      .then(protocol::I1("p1"))
      .then(protocol::I2("p1"))
      .then(protocol::I3("p1"))
      .then(protocol::I4("p1"))
      .then(protocol::I5("p1"))
      .then(protocol::I1("p2"))
      .then(protocol::I2("p2"))
      .expect(retIs("p2", false, Some("v1")))
      .expect(protocol::visibleSlotsByHeader(protocol::header.get(1), 1, "k1").size() == 1)
      .expect(protocol::inv)

  run concurrent_delete_get_interleaving =
    protocol::init
      .then(insertSuccess("p1", "k1", "v1"))
      .then(invokeDelete("p1", "k1"))
      .then(protocol::D1("p1"))
      .then(protocol::D2("p1"))
      .then(protocol::D3("p1"))
      .then(protocol::D4("p1"))
      .then(protocol::D5("p1"))
      .then(protocol::D6("p1"))
      .then(invokeGet("p2", "k1"))
      .then(protocol::G1("p2"))
      .then(protocol::D7("p1"))
      .then(protocol::G2("p2"))
      .expect(retIs("p2", false, None))
      .expect(protocol::op.get("p1") == Some(Delete) and protocol::pc.get("p1") == protocol::PcD8)
      .then(protocol::D8("p1"))
      .expect(protocol::op.get("p1") == None)
      .expect(protocol::inv)

  run concurrent_put_put_same_key =
    protocol::init
      .then(insertSuccess("p1", "k1", "v1"))
      .then(putToP6("p1", "k1", "v2"))
      .then(putToP6("p2", "k1", "v1"))
      .then(protocol::P7("p1"))
      .expect(retIs("p1", true, Some("v1")))
      .then(protocol::P7("p2"))
      .expect(protocol::pc.get("p2") == protocol::PcP4 and protocol::op.get("p2") == Some(Put))
      .then(protocol::P4("p2"))
      .then(protocol::P5("p2"))
      .then(protocol::P6("p2"))
      .then(protocol::P7("p2"))
      .expect(retIs("p2", true, Some("v2")))
      .expect(protocol::readVisibleByHeader(protocol::header.get(1), 1, "k1") == Some("v1"))
      .expect(protocol::inv)

  run put_delete_race_same_key =
    protocol::init
      .then(insertSuccess("p1", "k1", "v1"))
      .then(putToP6("p1", "k1", "v2"))
      .then(invokeDelete("p2", "k1"))
      .then(protocol::D1("p2"))
      .then(protocol::D2("p2"))
      .then(protocol::D3("p2"))
      .then(protocol::D4("p2"))
      .then(protocol::D5("p2"))
      .then(protocol::D6("p2"))
      .then(protocol::P7("p1"))
      .expect(retIs("p1", true, Some("v1")))
      .then(protocol::D7("p2"))
      .expect(protocol::pc.get("p2") == protocol::PcD4 and protocol::op.get("p2") == Some(Delete))
      .then(protocol::D4("p2"))
      .then(protocol::D5("p2"))
      .then(protocol::D6("p2"))
      .then(protocol::D7("p2"))
      .expect(retIs("p2", true, Some("v2")))
      .then(protocol::D8("p2"))
      .expect(protocol::readVisibleByHeader(protocol::header.get(1), 1, "k1") == None)
      .expect(protocol::inv)

  run insert_during_delete_window =
    protocol::init
      .then(insertSuccess("p1", "k1", "v1"))
      .then(deleteToD7("p1", "k1"))
      .expect(protocol::pc.get("p1") == protocol::PcD8 and protocol::op.get("p1") == Some(Delete))
      .then(invokeInsert("p2", "k1", "v2"))
      .then(protocol::I1("p2"))
      .then(protocol::I2("p2"))
      .expect(protocol::pc.get("p2") == protocol::PcI3 and protocol::ret.get("p2") == None)
      .then(protocol::I3("p2"))
      .then(protocol::I4("p2"))
      .then(protocol::I5("p2"))
      .expect(retIs("p2", true, None))
      .then(protocol::D8("p1"))
      .then(invokeGet("p1", "k1"))
      .then(protocol::G1("p1"))
      .then(protocol::G2("p1"))
      .expect(retIs("p1", true, Some("v2")))
      .expect(protocol::inv)

  run get_during_delete_window =
    protocol::init
      .then(insertSuccess("p1", "k1", "v1"))
      .then(deleteToD7("p1", "k1"))
      .then(invokeGet("p2", "k1"))
      .then(protocol::G1("p2"))
      .then(protocol::G2("p2"))
      .expect(retIs("p2", false, None))
      .expect(protocol::pc.get("p1") == protocol::PcD8 and protocol::op.get("p1") == Some(Delete))

  run i3_cas_failure_concurrent_reservation =
    protocol::init
      .then(invokeInsert("p1", "k1", "v1"))
      .then(invokeInsert("p2", "k2", "v2"))
      .then(protocol::I1("p1"))
      .then(protocol::I2("p1"))
      .then(protocol::I1("p2"))
      .then(protocol::I2("p2"))
      .then(protocol::I3("p1"))
      .expect(protocol::pc.get("p1") == protocol::PcI4)
      .then(protocol::I3("p2"))
      .expect(protocol::pc.get("p2") == protocol::PcI1 and protocol::op.get("p2") == Some(Insert) and protocol::ret.get("p2") == None)

  run i5_failure_then_i6_cleanup =
    protocol::init
      .then(invokeInsert("p1", "k1", "v1"))
      .then(protocol::I1("p1"))
      .then(protocol::I2("p1"))
      .then(protocol::I3("p1"))
      .then(protocol::I4("p1"))
      .expect(protocol::pc.get("p1") == protocol::PcI5)
      .then(invokeInsert("p2", "k2", "v2"))
      .then(protocol::I1("p2"))
      .then(protocol::I2("p2"))
      .then(protocol::I3("p2"))
      .then(protocol::I5("p1"))
      .expect(protocol::pc.get("p1") == protocol::PcI6)
      .then(protocol::I6("p1"))
      .expect(
        protocol::pc.get("p1") == protocol::PcI1 and
          protocol::header.get(1).slotState.get(protocol::local.get("p1").targetSlot) == protocol::Invalid and
          protocol::slotVal.get(1).get(protocol::local.get("p1").targetSlot) == None and
          protocol::slotTag.get(1).get(protocol::local.get("p1").targetSlot) == None
      )

  run p7_failure_from_delete_window_then_retry =
    protocol::init
      .then(insertSuccess("p1", "k1", "v1"))
      .then(putToP6("p1", "k1", "v2"))
      .then(invokeDelete("p2", "k1"))
      .then(protocol::D1("p2"))
      .then(protocol::D2("p2"))
      .then(protocol::D3("p2"))
      .then(protocol::D4("p2"))
      .then(protocol::D5("p2"))
      .then(protocol::D6("p2"))
      .then(protocol::D7("p2"))
      .expect(protocol::pc.get("p2") == protocol::PcD8 and protocol::op.get("p2") == Some(Delete))
      .then(protocol::P7("p1"))
      .expect(protocol::pc.get("p1") == protocol::PcP4 and protocol::op.get("p1") == Some(Put) and protocol::ret.get("p1") == None)
      .then(protocol::D8("p2"))
      .then(protocol::P4("p1"))
      .then(protocol::P5("p1"))
      .expect(protocol::pc.get("p1") == protocol::PcP1)
      .then(protocol::P1("p1"))
      .then(protocol::P2("p1"))
      .then(protocol::P3("p1"))
      .expect(retIs("p1", false, None))

  run i5_failure_then_i6_cleanup_same_key =
    protocol::init
      .then(invokeInsert("p1", "k1", "v1"))
      .then(protocol::I1("p1"))
      .then(protocol::I2("p1"))
      .then(protocol::I3("p1"))
      .then(protocol::I4("p1"))
      .expect(protocol::pc.get("p1") == protocol::PcI5)
      .then(invokeInsert("p2", "k1", "v2"))
      .then(protocol::I1("p2"))
      .then(protocol::I2("p2"))
      .then(protocol::I3("p2"))
      .then(protocol::I4("p2"))
      .then(protocol::I5("p2"))
      .expect(retIs("p2", true, None))
      .then(protocol::I5("p1"))
      .expect(protocol::pc.get("p1") == protocol::PcI6 and protocol::op.get("p1") == Some(Insert))
      .then(protocol::I6("p1"))
      .expect(protocol::pc.get("p1") == protocol::PcI1)
      .then(protocol::I1("p1"))
      .then(protocol::I2("p1"))
      .expect(retIs("p1", false, Some("v2")))
      .expect(
        protocol::visibleSlotsByHeader(protocol::header.get(1), 1, "k1").size() == 1 and
          protocol::readVisibleByHeader(protocol::header.get(1), 1, "k1") == Some("v2")
      )
      .expect(protocol::inv)

  run get_retries_on_header_change =
    protocol::init
      .then(insertSuccess("p1", "k1", "v1"))
      .then(invokeGet("p1", "k1"))
      .then(protocol::G1("p1"))
      .then(invokeInsert("p2", "k2", "v2"))
      .then(protocol::I1("p2"))
      .then(protocol::I2("p2"))
      .then(protocol::I3("p2"))
      .then(protocol::I4("p2"))
      .then(protocol::I5("p2"))
      .expect(retIs("p2", true, None))
      .then(protocol::G2("p1"))
      .expect(protocol::pc.get("p1") == protocol::PcG1 and protocol::op.get("p1") == Some(Get) and protocol::ret.get("p1") == None)
      .then(protocol::G1("p1"))
      .then(protocol::G2("p1"))
      .expect(retIs("p1", true, Some("v1")))

  // ------------------------ LoadOrCompute scenarios -----------------------

  // Absent key, save=true: commit via Shadow -> Valid.
  run load_or_compute_absent_save_true =
    protocol::init
      .then(invokeLoadOrCompute("p1", "k1", "v1", true))
      .then(protocol::LC1("p1"))
      .then(protocol::LC2("p1"))
      .then(protocol::LC3("p1"))
      .then(protocol::LC4("p1"))
      .then(protocol::LC5("p1"))
      .then(protocol::LC6("p1"))
      .then(protocol::LC7("p1"))
      .expect(retIs("p1", false, Some("v1")))
      .expect(protocol::readVisibleByHeader(protocol::header.get(1), 1, "k1") == Some("v1"))

  // Absent key, save=false: abort via Shadow -> Trying (LC7) -> Invalid (LC9); map unchanged.
  run load_or_compute_absent_save_false =
    protocol::init
      .then(invokeLoadOrCompute("p1", "k1", "v1", false))
      .then(protocol::LC1("p1"))
      .then(protocol::LC2("p1"))
      .then(protocol::LC3("p1"))
      .then(protocol::LC4("p1"))
      .then(protocol::LC5("p1"))
      .then(protocol::LC6("p1"))
      .then(protocol::LC7("p1"))
      // LC7 (save=false) is the linearization point: the result is posted here,
      // while op stays pending and pc advances to LC9 for cleanup.
      .expect(protocol::pc.get("p1") == protocol::PcLC9 and retIs("p1", false, Some("v1")))
      .then(protocol::LC9("p1"))
      .expect(retIs("p1", false, Some("v1")))
      .expect(protocol::readVisibleByHeader(protocol::header.get(1), 1, "k1") == None)

  // Key already present: LC2 takes the Valid-match return; fn is never reached.
  run load_or_compute_present_returns_loaded =
    protocol::init
      .then(insertSuccess("p1", "k1", "v1"))
      .then(invokeLoadOrCompute("p1", "k1", "v2", true))
      .then(protocol::LC1("p1"))
      .then(protocol::LC2("p1"))
      .expect(retIs("p1", true, Some("v1")))
      .expect(protocol::readVisibleByHeader(protocol::header.get(1), 1, "k1") == Some("v1"))

  // p2 sees p1's Shadow and waits until p1 commits.
  run load_or_compute_concurrent_waits_on_shadow =
    protocol::init
      .then(invokeLoadOrCompute("p1", "k1", "v1", true))
      .then(protocol::LC1("p1"))
      .then(protocol::LC2("p1"))
      .then(protocol::LC3("p1"))
      .then(protocol::LC4("p1"))
      .then(protocol::LC5("p1"))
      .expect(protocol::pc.get("p1") == protocol::PcLC6)
      .then(invokeLoadOrCompute("p2", "k1", "v2", true))
      .then(protocol::LC1("p2"))
      .then(protocol::LC2("p2"))
      .expect(protocol::pc.get("p2") == protocol::PcLC1 and protocol::op.get("p2") == Some(LoadOrCompute) and protocol::ret.get("p2") == None)
      .then(protocol::LC6("p1"))
      .then(protocol::LC7("p1"))
      .expect(retIs("p1", false, Some("v1")))
      .then(protocol::LC1("p2"))
      .then(protocol::LC2("p2"))
      .expect(retIs("p2", true, Some("v1")))
      .expect(protocol::inv)

  // Insert backs off when a Shadow holds its key; sees Valid after the commit.
  run insert_waits_on_shadow =
    protocol::init
      .then(invokeLoadOrCompute("p1", "k1", "v1", true))
      .then(protocol::LC1("p1"))
      .then(protocol::LC2("p1"))
      .then(protocol::LC3("p1"))
      .then(protocol::LC4("p1"))
      .then(protocol::LC5("p1"))
      .expect(protocol::pc.get("p1") == protocol::PcLC6)
      .then(invokeInsert("p2", "k1", "v2"))
      .then(protocol::I1("p2"))
      .then(protocol::I2("p2"))
      .expect(protocol::pc.get("p2") == protocol::PcI1 and protocol::op.get("p2") == Some(Insert) and protocol::ret.get("p2") == None)
      .then(protocol::LC6("p1"))
      .then(protocol::LC7("p1"))
      .expect(retIs("p1", false, Some("v1")))
      .then(protocol::I1("p2"))
      .then(protocol::I2("p2"))
      .expect(retIs("p2", false, Some("v1")))
      .expect(protocol::inv)

  run multiple_keys_same_bin =
    protocol::init
      .then(insertSuccess("p1", "k1", "v1"))
      .then(insertSuccess("p2", "k2", "v2"))
      .then(invokeGet("p1", "k1"))
      .then(protocol::G1("p1"))
      .then(protocol::G2("p1"))
      .expect(retIs("p1", true, Some("v1")))
      .then(invokeGet("p2", "k2"))
      .then(protocol::G1("p2"))
      .then(protocol::G2("p2"))
      .expect(retIs("p2", true, Some("v2")))
      .expect(
        protocol::visibleSlotsByHeader(protocol::header.get(1), 1, "k1").size() == 1 and
          protocol::visibleSlotsByHeader(protocol::header.get(1), 1, "k2").size() == 1
      )
      .expect(protocol::inv)

  // LC8 (unreserve after an LC5 CAS failure): p1 reserves and fills slot 1,
  // then p2's full insert bumps the bin-1 header twice (I3 reserve + I5
  // finalize), so p1's LC5 expectedH check fails and p1 must clear its
  // Trying reservation via LC8 and restart from LC1. fn never ran; ret stays
  // unset.
  run lc5_failure_then_lc8_cleanup =
    protocol::init
      .then(invokeLoadOrCompute("p1", "k1", "v1", true))
      .then(protocol::LC1("p1"))
      .then(protocol::LC2("p1"))
      .then(protocol::LC3("p1"))
      .then(protocol::LC4("p1"))
      .then(insertSuccess("p2", "k2", "v2"))
      .then(protocol::LC5("p1"))
      .expect(protocol::pc.get("p1") == protocol::PcLC8 and protocol::ret.get("p1") == None)
      .then(protocol::LC8("p1"))
      .expect(
        protocol::header.get(1).slotState.get(protocol::local.get("p1").targetSlot) == protocol::Invalid and
          protocol::slotVal.get(1).get(protocol::local.get("p1").targetSlot) == None and
          protocol::slotTag.get(1).get(protocol::local.get("p1").targetSlot) == None and
          protocol::ret.get("p1") == None and
          protocol::pc.get("p1") == protocol::PcLC1
      )
      .expect(protocol::inv)

  // D3 absent-return on an empty map: D2 computes targetSlot == 0 (nothing
  // visible), so D3's keyAbsentGuard fires and Delete returns (false, None).
  // Exercises the absentReturnConsistent path directly.
  run delete_absent_fails =
    protocol::init
      .then(invokeDelete("p1", "k1"))
      .then(protocol::D1("p1"))
      .then(protocol::D2("p1"))
      .then(protocol::D3("p1"))
      .expect(retIs("p1", false, None))
      .expect(protocol::inv)

  // A Shadow slot is invisible to Delete's scan: while p1's compute is in
  // flight (Shadow published at LC5, commit pending), p2's Delete takes the
  // D3 absent-return. After p1's LC7 commit, the key is visible to Get.
  run put_delete_absent_during_shadow =
    protocol::init
      .then(invokeLoadOrCompute("p1", "k1", "v1", true))
      .then(protocol::LC1("p1"))
      .then(protocol::LC2("p1"))
      .then(protocol::LC3("p1"))
      .then(protocol::LC4("p1"))
      .then(protocol::LC5("p1"))
      .then(invokeDelete("p2", "k1"))
      .then(protocol::D1("p2"))
      .then(protocol::D2("p2"))
      .then(protocol::D3("p2"))
      .expect(retIs("p2", false, None))
      .then(protocol::LC6("p1"))
      .then(protocol::LC7("p1"))
      .expect(retIs("p1", false, Some("v1")))
      .then(invokeGet("p2", "k1"))
      .then(protocol::G1("p2"))
      .then(protocol::G2("p2"))
      .expect(retIs("p2", true, Some("v1")))
      .expect(protocol::inv)

  // LC7 commit-CAS failure: p2's insert moves the header after p1's LC6
  // hLoop load, so LC7 bounces back to LC6 (no return posted); the reloaded
  // hLoop then lets the commit succeed on the retry.
  run lc7_drift_retries_lc6 =
    protocol::init
      .then(invokeLoadOrCompute("p1", "k1", "v1", true))
      .then(protocol::LC1("p1"))
      .then(protocol::LC2("p1"))
      .then(protocol::LC3("p1"))
      .then(protocol::LC4("p1"))
      .then(protocol::LC5("p1"))
      .then(protocol::LC6("p1"))
      .then(insertSuccess("p2", "k2", "v2"))
      .then(protocol::LC7("p1"))
      .expect(protocol::pc.get("p1") == protocol::PcLC6 and protocol::ret.get("p1") == None)
      .then(protocol::LC6("p1"))
      .then(protocol::LC7("p1"))
      .expect(retIs("p1", false, Some("v1")))
      .expect(protocol::readVisibleByHeader(protocol::header.get(1), 1, "k1") == Some("v1"))
      .expect(protocol::inv)

  // D5 retry forced by a concurrent delete's D7: p2 scans slot 1 while it is
  // still visible (D1/D2), p1's D7 then clears val+tag (header-silent), so
  // p2's D3 header re-check still passes but D5 sees the dead value and
  // restarts from D1. p1 is left mid-op at PcD8 (the open delete window);
  // inv must still hold there.
  run d5_retry_via_delete_window =
    protocol::init
      .then(insertSuccess("p1", "k1", "v1"))
      .then(invokeDelete("p1", "k1"))
      .then(protocol::D1("p1"))
      .then(protocol::D2("p1"))
      .then(protocol::D3("p1"))
      .then(protocol::D4("p1"))
      .then(protocol::D5("p1"))
      .then(protocol::D6("p1"))
      .then(invokeDelete("p2", "k1"))
      .then(protocol::D1("p2"))
      .then(protocol::D2("p2"))
      .then(protocol::D7("p1"))
      .then(protocol::D3("p2"))
      .then(protocol::D4("p2"))
      .then(protocol::D5("p2"))
      .expect(protocol::pc.get("p2") == protocol::PcD1)
      .expect(protocol::inv)

  // --------------------- LoadOrCompute contract scenarios -----------------
  // Companions to the contract corollaries in invariants.qnt
  // (contractAtMostOneComputePerKey / contractHolderExclusive): the corollary
  // checks prove the fn window (PcLC6/PcLC7) is exclusive per key over ALL
  // inv-states; these scenarios pin the intended behavior at its boundaries.

  // Post-abort handoff: the SECOND compute may legitimately run after the
  // first resolves. p1 (save=false) holds the Shadow; p2 waits at LC2; p1
  // aborts (LC7 posts ret, LC9 frees slot 1); p2 then acquires Shadow itself
  // and commits v2. At-most-once-per-key is about CONCURRENT fn runs —
  // sequential re-execution after an abort is the contract working as
  // intended.
  run lc_abort_handoff_second_compute_runs =
    protocol::init
      .then(invokeLoadOrCompute("p1", "k1", "v1", false))
      .then(protocol::LC1("p1"))
      .then(protocol::LC2("p1"))
      .then(protocol::LC3("p1"))
      .then(protocol::LC4("p1"))
      .then(protocol::LC5("p1"))
      .expect(protocol::pc.get("p1") == protocol::PcLC6)
      .then(invokeLoadOrCompute("p2", "k1", "v2", true))
      .then(protocol::LC1("p2"))
      .then(protocol::LC2("p2"))
      // p2 sees p1's Shadow on k1 and backs off to LC1 (no slot picked).
      .expect(protocol::pc.get("p2") == protocol::PcLC1 and protocol::ret.get("p2") == None)
      .then(protocol::LC6("p1"))
      .then(protocol::LC7("p1"))
      // save=false abort phase 1: ret posted at LC7 (linearization point),
      // slot 1 Shadow -> Trying, cleanup pending at PcLC9.
      .expect(protocol::pc.get("p1") == protocol::PcLC9 and retIs("p1", false, Some("v1")))
      .then(protocol::LC9("p1"))
      .expect(protocol::header.get(1).slotState.get(1) == protocol::Invalid)
      .then(protocol::LC1("p2"))
      .then(protocol::LC2("p2"))
      .then(protocol::LC3("p2"))
      .then(protocol::LC4("p2"))
      .then(protocol::LC5("p2"))
      .then(protocol::LC6("p2"))
      .then(protocol::LC7("p2"))
      .expect(retIs("p1", false, Some("v1")))
      .expect(retIs("p2", false, Some("v2")))
      .expect(protocol::readVisibleByHeader(protocol::header.get(1), 1, "k1") == Some("v2"))
      .expect(protocol::inv)

  // Invocation overlap is NOT fn overlap: p1 (save=false) is parked at PcLC9
  // (resolved-but-not-returned — ret was posted at LC7, which also flipped
  // its slot Shadow -> Trying). A second same-key compute may enter the fn
  // window WHILE p1 still sits there: p2's LC2 sees neither a visible nor a
  // Shadow slot for k1 (slot 1 is Trying), so the free-slot pick skips the
  // Trying slot 1 and takes slot 2 (minOr0 of {2,3}). This is exactly why the
  // corollary's window is {PcLC6,PcLC7} and NOT {..,PcLC9} — widening it is
  // the recorded [violation] negative control.
  run lc9_window_second_compute_overlaps =
    protocol::init
      .then(invokeLoadOrCompute("p1", "k1", "v1", false))
      .then(protocol::LC1("p1"))
      .then(protocol::LC2("p1"))
      .then(protocol::LC3("p1"))
      .then(protocol::LC4("p1"))
      .then(protocol::LC5("p1"))
      .then(protocol::LC6("p1"))
      .then(protocol::LC7("p1"))
      .expect(protocol::pc.get("p1") == protocol::PcLC9 and retIs("p1", false, Some("v1")))
      .then(invokeLoadOrCompute("p2", "k1", "v2", true))
      .then(protocol::LC1("p2"))
      .then(protocol::LC2("p2"))
      .expect(protocol::local.get("p2").targetSlot == 2)
      .then(protocol::LC3("p2"))
      .then(protocol::LC4("p2"))
      .then(protocol::LC5("p2"))
      // The overlap window: p2 holds Shadow (slot 2) while p1 is still at
      // PcLC9 with its aborted claim (slot 1, Trying).
      .expect(
        protocol::pc.get("p1") == protocol::PcLC9 and
          protocol::pc.get("p2") == protocol::PcLC6 and
          protocol::header.get(1).slotState.get(1) == protocol::Trying and
          protocol::header.get(1).slotState.get(2) == protocol::Shadow
      )
      .then(protocol::LC9("p1"))
      .then(protocol::LC6("p2"))
      .then(protocol::LC7("p2"))
      .expect(retIs("p1", false, Some("v1")))
      .expect(retIs("p2", false, Some("v2")))
      .expect(protocol::readVisibleByHeader(protocol::header.get(1), 1, "k1") == Some("v2"))
      .expect(protocol::inv)

  // The ONE header write that changes bits without bumping the version (D8's
  // clearSlotState) must still defeat LC7's commit CAS: p1's LC6 captures
  // hLoop with k2's slot 1 still Valid; p2's D7 (header-silent val/tag clear)
  // then D8 (Valid -> Invalid, version UNCHANGED) flip the slot state under
  // the same version; p1's LC7 compares the whole header RECORD, so it
  // catches the drift (slotState differs though version is equal), bounces to
  // LC6 without returning, reloads, and commits on the retry.
  run lc7_drift_via_d8_version_silent_clear =
    protocol::init
      .then(insertSuccess("p1", "k2", "v2"))
      .then(invokeLoadOrCompute("p1", "k1", "v1", true))
      .then(protocol::LC1("p1"))
      .then(protocol::LC2("p1"))
      .then(protocol::LC3("p1"))
      .then(protocol::LC4("p1"))
      .then(protocol::LC5("p1"))
      // p1 holds Shadow on slot 2 (slot 1 is k2's Valid entry).
      .expect(protocol::pc.get("p1") == protocol::PcLC6 and protocol::header.get(1).slotState.get(2) == protocol::Shadow)
      .then(invokeDelete("p2", "k2"))
      .then(protocol::D1("p2"))
      .then(protocol::D2("p2"))
      .then(protocol::D3("p2"))
      .then(protocol::D4("p2"))
      .then(protocol::D5("p2"))
      .then(protocol::D6("p2"))
      .then(protocol::D7("p2"))
      // D7 cleared slot 1's val/tag fully header-silently: still Valid, version 4.
      .expect(
        protocol::pc.get("p2") == protocol::PcD8 and
          protocol::header.get(1).version == 4 and
          protocol::header.get(1).slotState.get(1) == protocol::Valid
      )
      .then(protocol::LC6("p1"))
      .then(protocol::D8("p2"))
      // D8's AND: slot 1 Valid -> Invalid with NO version bump.
      .expect(
        protocol::header.get(1).version == 4 and
          protocol::header.get(1).slotState.get(1) == protocol::Invalid
      )
      .then(protocol::LC7("p1"))
      // lc7SuccessGuard compares the whole header record against hLoop:
      // versions agree (4 == 4) but slot 1's state differs -> drift bounce,
      // no return posted.
      .expect(protocol::pc.get("p1") == protocol::PcLC6 and protocol::ret.get("p1") == None)
      .then(protocol::LC6("p1"))
      .then(protocol::LC7("p1"))
      .expect(retIs("p1", false, Some("v1")))
      .expect(protocol::readVisibleByHeader(protocol::header.get(1), 1, "k1") == Some("v1"))
      .expect(retIs("p2", true, Some("v2")))
      .expect(protocol::inv)
}
