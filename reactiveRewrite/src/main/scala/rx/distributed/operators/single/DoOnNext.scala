package rx.distributed.operators.single

import rx.lang.scala.Observable

case class DoOnNext[T](action: T => Unit) extends SingleOperator[T, T] {
  override def toObservable(left: Observable[Any]): Observable[Any] = {
    left.doOnNext(v => action(v.asInstanceOf[T]))
  }
}