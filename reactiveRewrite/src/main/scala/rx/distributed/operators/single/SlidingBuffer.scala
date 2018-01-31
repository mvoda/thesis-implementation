package rx.distributed.operators.single

import rx.lang.scala.Observable

case class SlidingBuffer[T](count: Int, skip: Int) extends SingleOperator[T, Seq[T]] {
  override def toObservable(left: Observable[Any]): Observable[Any] = {
    left.slidingBuffer(count, skip)
  }
}
