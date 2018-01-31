package rx.distributed.operators.errorHandling

import rx.distributed.operators.Operator
import rx.lang.scala.Observable

case class OnErrorResumeNext[T, S >: T](onError: Throwable => (String, Seq[Operator])) extends ErrorHandlingOperator[T, S, S] {
  override def toObservable(left: Observable[Any], resume: Throwable => Observable[Any]): Observable[Any] = left.onErrorResumeNext(resume)
}
