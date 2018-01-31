package rx.distributed.operators.errorHandling

import rx.distributed.operators.Operator
import rx.lang.scala.Observable

trait ErrorHandlingOperator[T, S, R] extends Operator {
  def onError: Throwable => (String, Seq[Operator])

  def toObservable(left: Observable[Any], resume: Throwable => Observable[Any]): Observable[Any]
}
