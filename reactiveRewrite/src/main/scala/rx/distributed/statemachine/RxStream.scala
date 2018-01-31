package rx.distributed.statemachine

import rx.distributed.notifications.RxNotification
import rx.distributed.observers.notifications.NotificationObserver
import rx.distributed.raft.statemachine.output.StatemachineOutput
import rx.lang.scala.Observer

case class RxStream private(notificationObserver: NotificationObserver[Any], outputObserver: Observer[RxNotification[Any]]) {
  val streamInput: StreamInput[Any] = StreamInput(notificationObserver.subscriber)

  def isUnsubscribed: Boolean = streamInput.isUnsubscribed

  def output: StatemachineOutput = notificationObserver.output

  def inputObserver: Observer[RxNotification[Any]] = streamInput.observer
}

object RxStream {
  def apply(observer: NotificationObserver[Any]): RxStream =
    RxStream(observer, observer.subscriber)

  def apply(rxStream: RxStream): RxStream = {
    RxStream(rxStream.notificationObserver, rxStream.inputObserver)
  }
}
