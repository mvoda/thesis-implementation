package rx.distributed.operators.single

import rx.lang.scala.{Observable, Scheduler}

case class ObserveOn[T](schedulerLambda: () => Scheduler) extends SingleOperator[T, T] {
  override def toObservable(left: Observable[Any]): Observable[Any] = {
    left.observeOn(schedulerLambda())
  }
}