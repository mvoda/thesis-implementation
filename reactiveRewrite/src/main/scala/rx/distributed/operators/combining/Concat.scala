package rx.distributed.operators.combining

import rx.lang.scala.Observable

case class Concat[T, S >: T](operatorId: String, streamPosition: StreamPosition) extends CombiningOperator[T, S, S] {
  override def toObservable(left: Observable[Any], right: Observable[Any]): Observable[Any] = left ++ right
}
