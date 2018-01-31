package rx.distributed.operators.combining

import rx.lang.scala.Observable

case class CombineLatestWith[T, S, R](operatorId: String, streamPosition: StreamPosition, combiner: (T, S) => R) extends CombiningOperator[T, S, R] {
  override def toObservable(left: Observable[Any], right: Observable[Any]): Observable[Any] =
    left.combineLatestWith(right)((left, right) => combiner(left.asInstanceOf[T], right.asInstanceOf[S]))
}