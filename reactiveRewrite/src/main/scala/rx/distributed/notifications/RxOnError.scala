package rx.distributed.notifications

import rx.lang.scala.Notification
import rx.lang.scala.Notification.OnError

case class RxOnError[T](error: scala.Throwable) extends RxNotification[T] {
  override def toNotification: Notification[T] = OnError(error)
}