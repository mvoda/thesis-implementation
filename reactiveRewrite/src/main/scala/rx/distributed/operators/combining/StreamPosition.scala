package rx.distributed.operators.combining

sealed trait StreamPosition

case class LeftStream() extends StreamPosition

case class RightStream() extends StreamPosition
