package rx.distributed.operators.single

import rx.lang.scala.Observable

case class Filter[T](predicate: (T) => Boolean) extends SingleOperator[T, T] {
  override def toObservable(left: Observable[Any]): Observable[Any] = {
    left.filter(x => predicate(x.asInstanceOf[T]))
  }
}
