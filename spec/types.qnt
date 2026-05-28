module types {
  type PROC = str
  type KEY = str
  type VAL = str

  type Option[t] = | Some(t) | None

  type OpName = | Get | Insert | Put | Delete | LoadOrCompute
  type Op = Option[OpName]

  // The (VAL, bool) in LoadComputeArg is fn's (value, save) outcome, nondet per invocation.
  type Arg = | KeyArg(KEY) | KeyValArg((KEY, VAL)) | LoadComputeArg((KEY, VAL, bool))
  type Res = { ok: bool, resVal: Option[VAL] }

  type State = KEY -> Option[VAL]
  type Config = {
    state: State,
    op: PROC -> Op,
    arg: PROC -> Option[Arg],
    res: PROC -> Option[Res],
  }

  const procSet: Set[PROC]
  const keyDomain: Set[KEY]
  const valDomain: Set[VAL]

  pure val opNames: Set[OpName] = Set(Get, Insert, Put, Delete, LoadOrCompute)

  pure def keyArgs: Set[Arg] = keyDomain.map(k => KeyArg(k))
  pure def keyValArgs: Set[Arg] = keyDomain.map(k => valDomain.map(v => KeyValArg((k, v)))).flatten()
  pure def keyValSaveArgs: Set[Arg] =
    keyDomain.map(k =>
      valDomain.map(v =>
        Set(true, false).map(save => LoadComputeArg((k, v, save)))
      ).flatten()
    ).flatten()

  pure def argsOf(op: Op): Set[Arg] = {
    match op {
      | Some(opname) =>
          match opname {
            | Get => keyArgs
            | Insert => keyValArgs
            | Put => keyValArgs
            | Delete => keyArgs
            | LoadOrCompute => keyValSaveArgs
          }
      | None => Set()
    }
  }

  // Single source of truth for abstract operation effects.
  // Non-enabled combinations conservatively return c unchanged.
  pure def deltaResult(c: Config, p: PROC): Config = {
    val opP = c.op.get(p)
    val argP = c.arg.get(p)
    val resP = c.res.get(p)

    if (opP == Some(Get) and resP == None) {
      match argP {
        | Some(a) =>
            match a {
              | KeyArg(k) =>
                  { ...c, res: c.res.set(p, Some({ ok: c.state.get(k) != None, resVal: c.state.get(k) })) }
              | _ => c
            }
        | _ => c
      }
    } else if (opP == Some(Insert) and resP == None) {
      match argP {
        | Some(a) =>
            match a {
              | KeyValArg(kv) =>
                  {
                    val k = kv._1
                    val v = kv._2
                    if (c.state.get(k) != None) {
                      { ...c, res: c.res.set(p, Some({ ok: false, resVal: c.state.get(k) })) }
                    } else {
                      { ...c, state: c.state.set(k, Some(v)), res: c.res.set(p, Some({ ok: true, resVal: None })) }
                    }
                  }
              | _ => c
            }
        | _ => c
      }
    } else if (opP == Some(Put) and resP == None) {
      match argP {
        | Some(a) =>
            match a {
              | KeyValArg(kv) =>
                  {
                    val k = kv._1
                    val v = kv._2
                    if (c.state.get(k) == None) {
                      { ...c, res: c.res.set(p, Some({ ok: false, resVal: None })) }
                    } else {
                      { ...c, state: c.state.set(k, Some(v)), res: c.res.set(p, Some({ ok: true, resVal: c.state.get(k) })) }
                    }
                  }
              | _ => c
            }
        | _ => c
      }
    } else if (opP == Some(Delete) and resP == None) {
      match argP {
        | Some(a) =>
            match a {
              | KeyArg(k) =>
                  if (c.state.get(k) == None) {
                    { ...c, res: c.res.set(p, Some({ ok: false, resVal: None })) }
                  } else {
                    { ...c, state: c.state.set(k, None), res: c.res.set(p, Some({ ok: true, resVal: c.state.get(k) })) }
                  }
              | _ => c
            }
        | _ => c
      }
    } else if (opP == Some(LoadOrCompute) and resP == None) {
      // ok=true: key already present, existing returned. ok=false: fn ran.
      match argP {
        | Some(a) =>
            match a {
              | LoadComputeArg(triple) =>
                  {
                    val k = triple._1
                    val v = triple._2
                    val save = triple._3
                    if (c.state.get(k) != None) {
                      { ...c, res: c.res.set(p, Some({ ok: true, resVal: c.state.get(k) })) }
                    } else if (save) {
                      { ...c, state: c.state.set(k, Some(v)),
                        res: c.res.set(p, Some({ ok: false, resVal: Some(v) })) }
                    } else {
                      { ...c, res: c.res.set(p, Some({ ok: false, resVal: Some(v) })) }
                    }
                  }
              | _ => c
            }
        | _ => c
      }
    } else {
      c
    }
  }

  pure def deltaEnabled(c: Config, p: PROC): bool = {
    val opP = c.op.get(p)
    val argP = c.arg.get(p)
    c.res.get(p) == None and
      match argP {
        | Some(a) => argsOf(opP).contains(a)
        | _ => false
      }
  }

}
