package rx.distributed.operators.single

import rx.lang.scala.Observable

case class Map[T, S](mapper: (T) => S) extends SingleOperator[T, S] {
  override def toObservable(left: Observable[Any]): Observable[Any] = {
    left.map(x => mapper(x.asInstanceOf[T]))
  }
}
