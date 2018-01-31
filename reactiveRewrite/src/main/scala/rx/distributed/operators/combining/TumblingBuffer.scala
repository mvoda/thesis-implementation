package rx.distributed.operators.combining

import rx.lang.scala.Observable

case class TumblingBuffer[T, S](operatorId: String, streamPosition: StreamPosition) extends CombiningOperator[T, S, Seq[T]] {
  override def toObservable(left: Observable[Any], right: Observable[Any]): Observable[Any] = left.tumblingBuffer(right)
}
