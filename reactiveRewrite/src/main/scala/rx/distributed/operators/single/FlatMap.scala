package rx.distributed.operators.single

import rx.lang.scala.Observable

case class FlatMap[T, S](fun: (T) => Observable[S]) extends SingleOperator[T, S] {
  override def toObservable(left: Observable[Any]): Observable[Any] = {
    left.flatMap(v => fun(v.asInstanceOf[T]))
  }
}
