module probe4_export_main {
  import outer(n = 5) as m from "./probe4_export"

  // Must resolve through the export: inner's action, def, AND type constructor.
  // NOTE: constructors must be referenced qualified (m::Red) through a
  // parameterized import-as — a bare `Red` does not resolve. Defs/actions/types
  // (m::positive, m::init, m::Color) DO resolve transitively, which is all `inv` needs.
  action init = m::init
  action step = m::step
  def inv = m::positive and m::alsoPositive and (m::Red == m::Red)

  pure val c: m::Color = m::Red
}
