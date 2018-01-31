package rx.distributed.operators.single

import rx.lang.scala.Observable

case class Retry[T]() extends SingleOperator[T, T] {
  override def toObservable(left: Observable[Any]): Observable[Any] = left.retry
}

