module inner {
  const n: int
  type Color = | Red | Blue
  var x: int
  action init = x' = n
  action step = x' = x + 1
  def positive = x > 0
}

module outer {
  import inner.* from "./probe4_export"
  export inner.*
  def alsoPositive = positive
}
