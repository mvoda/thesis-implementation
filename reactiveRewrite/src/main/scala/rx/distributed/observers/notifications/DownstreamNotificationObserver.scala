package rx.distributed.observers.notifications

import rx.distributed.notifications.{RxNotification, RxOnCompleted, RxOnError}
import rx.distributed.observers.statemachine.DownstreamStatemachineObserver
import rx.distributed.raft.converters.ObserverConverters
import rx.distributed.raft.statemachine.output.StatemachineOutput
import rx.distributed.statemachine.commands.RxEvent
import rx.distributed.statemachine.responses.ForwardingEmpty
import rx.lang.scala.Subscriber

import scala.collection.mutable

case class DownstreamNotificationObserver[T](observer: DownstreamStatemachineObserver) extends NotificationObserver[T] {
  private val _values: scala.collection.mutable.MutableList[RxNotification[T]] = mutable.MutableList[RxNotification[T]]()

  override def output: StatemachineOutput = {
    val event = ForwardingEmpty(observer.cluster, _values.map(value => RxEvent(observer.streamId, value)))
    _values.clear()
    event
  }

  //TODO: this could push 2 onCompleted, 2 onError, etc. is this a problem?
  override val subscriber: Subscriber[RxNotification[T]] = ObserverConverters.makeSafe(Subscriber(
    (rxNotification: RxNotification[T]) => _values += rxNotification,
    (error: Throwable) => _values += RxOnError(error),
    () => _values += RxOnCompleted()))
}
