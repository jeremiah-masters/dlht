// Cross-bin scenarios. The main suite (scenarios.qnt) runs at numBins = 1,
// where hashConsistency and every cross-bin frame condition are vacuous; this
// module re-instantiates the protocol with two bins (k1 -> bin 1, k2 -> bin 2)
// so that per-bin isolation is actually exercised: an op on one bin must leave
// the other bin's header, slots, and open windows untouched.
module scenarios_2bin {
  import types(
    procSet = Set("p1", "p2"),
    keyDomain = Set("k1", "k2"),
    valDomain = Set("v1", "v2"),
  ).* from "../types"

  import checked(
    procSet = Set("p1", "p2"),
    keyDomain = Set("k1", "k2"),
    valDomain = Set("v1", "v2"),
    numBins = 2,
    slotsPerBin = 3,
    hash = Map("k1" -> 1, "k2" -> 2),
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

  // Inserts into different bins do not interact: both succeed, each key is
  // visible in its own bin, and each Get resolves against its own header.
  run cross_bin_inserts_independent =
    protocol::init
      .then(insertSuccess("p1", "k1", "v1"))
      .then(insertSuccess("p2", "k2", "v2"))
      .expect(retIs("p1", true, None) and retIs("p2", true, None))
      .then(invokeGet("p1", "k1"))
      .then(protocol::G1("p1"))
      .then(protocol::G2("p1"))
      .expect(retIs("p1", true, Some("v1")))
      .then(invokeGet("p2", "k2"))
      .then(protocol::G1("p2"))
      .then(protocol::G2("p2"))
      .expect(retIs("p2", true, Some("v2")))
      .expect(
        protocol::readVisibleByHeader(protocol::header.get(1), 1, "k1") == Some("v1") and
          protocol::readVisibleByHeader(protocol::header.get(2), 2, "k2") == Some("v2")
      )
      .expect(protocol::inv)

  // p1 holds bin 1's delete window open (D7 done, D8 pending: slot Valid with
  // val/tag cleared) while p2 reads k2 from bin 2 — bin 2's header and slot
  // are untouched by the window, so the Get returns normally mid-window.
  run cross_bin_delete_window_isolated =
    protocol::init
      .then(insertSuccess("p2", "k2", "v2"))
      .then(insertSuccess("p1", "k1", "v1"))
      .then(deleteToD7("p1", "k1"))
      .expect(
        protocol::pc.get("p1") == protocol::PcD8 and
          protocol::header.get(1).slotState.get(protocol::local.get("p1").targetSlot) == protocol::Valid and
          protocol::slotVal.get(1).get(protocol::local.get("p1").targetSlot) == None
      )
      .then(invokeGet("p2", "k2"))
      .then(protocol::G1("p2"))
      .then(protocol::G2("p2"))
      .expect(retIs("p2", true, Some("v2")))
      .expect(protocol::readVisibleByHeader(protocol::header.get(2), 2, "k2") == Some("v2"))
      .then(protocol::D8("p1"))
      .expect(
        protocol::readVisibleByHeader(protocol::header.get(1), 1, "k1") == None and
          protocol::readVisibleByHeader(protocol::header.get(2), 2, "k2") == Some("v2")
      )
      .expect(protocol::inv)

  // Two delete windows may be open simultaneously, one per bin: both procs sit
  // at PcD8 with their resolved returns posted, and inv (deleteWindowOwned,
  // d8WindowOwnerUnique, retAtD8) holds across the pair before either D8 runs.
  run cross_bin_concurrent_delete_windows =
    protocol::init
      .then(insertSuccess("p1", "k1", "v1"))
      .then(insertSuccess("p2", "k2", "v2"))
      .then(deleteToD7("p1", "k1"))
      .then(deleteToD7("p2", "k2"))
      .expect(
        protocol::pc.get("p1") == protocol::PcD8 and
          protocol::pc.get("p2") == protocol::PcD8 and
          retIs("p1", true, Some("v1")) and
          retIs("p2", true, Some("v2"))
      )
      .expect(protocol::inv)
      .then(protocol::D8("p1"))
      .then(protocol::D8("p2"))
      .expect(
        protocol::readVisibleByHeader(protocol::header.get(1), 1, "k1") == None and
          protocol::readVisibleByHeader(protocol::header.get(2), 2, "k2") == None
      )
      .expect(protocol::inv)
}
