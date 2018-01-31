package rx.distributed.operators.single

import rx.lang.scala.Observable

case class Take[T](n: Int) extends SingleOperator[T, T] {
  override def toObservable(left: Observable[Any]): Observable[Any] = {
    left.take(n)
  }
}
