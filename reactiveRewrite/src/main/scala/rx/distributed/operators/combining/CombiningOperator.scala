package rx.distributed.operators.combining

import rx.distributed.operators.Operator
import rx.lang.scala.Observable

trait CombiningOperator[T, S, R] extends Operator {
  def operatorId: String

  def streamPosition: StreamPosition

  def toObservable(left: Observable[Any], right: Observable[Any]): Observable[Any]
}
