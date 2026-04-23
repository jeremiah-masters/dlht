module scenarios {
  import types(
    procSet = Set("p1", "p2"),
    keyDomain = Set("k1", "k2"),
    valDomain = Set("v1", "v2"),
  ).* from "../types"

  import protocol(
    procSet = Set("p1", "p2"),
    keyDomain = Set("k1", "k2"),
    valDomain = Set("v1", "v2"),
    numBins = 1,
    slotsPerBin = 3,
    hash = Map("k1" -> 1, "k2" -> 1),
  ) as protocol from "../protocol"

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
      .then(protocol::I1("p2"))
      .then(protocol::I2("p2"))
      .expect(retIs("p2", false, Some("v1")))
      .expect(protocol::visibleSlotsByHeader(protocol::header.get(1), 1, "k1").size() == 1)

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
}
