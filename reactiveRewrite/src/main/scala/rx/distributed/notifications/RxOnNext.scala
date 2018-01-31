package rx.distributed.notifications

import rx.lang.scala.Notification
import rx.lang.scala.Notification.OnNext

case class RxOnNext[T](value: T) extends RxNotification[T] {
  override def toNotification: Notification[T] = OnNext(value)
}
