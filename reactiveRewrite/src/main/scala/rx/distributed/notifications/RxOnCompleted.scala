package rx.distributed.notifications

import rx.lang.scala.Notification
import rx.lang.scala.Notification.OnCompleted

case class RxOnCompleted[T]() extends RxNotification[T] {
  override def toNotification: Notification[T] = OnCompleted
}

