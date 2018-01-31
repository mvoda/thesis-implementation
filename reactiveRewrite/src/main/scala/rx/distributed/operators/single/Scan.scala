package rx.distributed.operators.single

import rx.lang.scala.Observable

case class Scan[T, S](initialValue: S)(accumulator: (S, T) => S) extends SingleOperator[T, S] {
  override def toObservable(left: Observable[Any]): Observable[Any] = {
    left.scan(initialValue)((s, t) => accumulator(s, t.asInstanceOf[T]))
  }
}