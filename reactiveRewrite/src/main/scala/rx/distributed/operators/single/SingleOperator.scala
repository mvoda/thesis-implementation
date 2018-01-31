package rx.distributed.operators.single

import rx.distributed.operators.Operator
import rx.lang.scala.Observable

trait SingleOperator[T, S] extends Operator {
  def toObservable(upstream: Observable[Any]): Observable[Any]
}
