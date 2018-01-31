package rx.distributed.notifications

import rx.lang.scala.Notification
import rx.lang.scala.Notification.{OnCompleted, OnError, OnNext}

trait RxNotification[T] extends Serializable {
  def toNotification: Notification[T]
}

object RxNotification {
  def apply[T](notification: Notification[T]): RxNotification[T] = notification match {
    case OnNext(value) => RxOnNext(value)
    case OnError(throwable) => RxOnError[T](throwable)
    case OnCompleted => RxOnCompleted[T]()
  }
}
