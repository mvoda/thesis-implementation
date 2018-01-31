package rx.distributed.observers.notifications

import rx.distributed.notifications.RxNotification
import rx.distributed.observers.statemachine.{DownstreamStatemachineObserver, LocalStatemachineObserver, StatemachineObserver}
import rx.distributed.raft.statemachine.output.StatemachineOutput
import rx.lang.scala.Subscriber

trait NotificationObserver[T] {
  def subscriber: Subscriber[RxNotification[T]]

  def output: StatemachineOutput
}

object NotificationObserver {
  def apply(observer: StatemachineObserver): NotificationObserver[Any] = observer match {
    case localObs: LocalStatemachineObserver => LocalNotificationObserver(localObs)
    case remoteObs: DownstreamStatemachineObserver => DownstreamNotificationObserver(remoteObs)
  }
}
