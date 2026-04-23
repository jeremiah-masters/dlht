module common {
  pure def stepClassCover(invokingStep: bool, intermediateStep: bool, returningStep: bool): bool =
    invokingStep or intermediateStep or returningStep

  pure def stepClassDisjoint(invokingStep: bool, intermediateStep: bool, returningStep: bool): bool =
    not(invokingStep and intermediateStep) and
      not(invokingStep and returningStep) and
      not(intermediateStep and returningStep)
}
