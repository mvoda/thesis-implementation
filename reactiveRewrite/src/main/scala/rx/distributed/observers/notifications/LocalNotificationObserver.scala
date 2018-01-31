package rx.distributed.observers.notifications

import rx.distributed.notifications.{RxNotification, RxOnCompleted, RxOnError, RxOnNext}
import rx.distributed.observers.statemachine.LocalStatemachineObserver
import rx.distributed.raft.converters.ObserverConverters
import rx.distributed.raft.statemachine.output.StatemachineOutput
import rx.distributed.statemachine.responses.Empty
import rx.lang.scala.Subscriber

case class LocalNotificationObserver[T](observer: LocalStatemachineObserver) extends NotificationObserver[T] {
  def output: StatemachineOutput = Empty()

  private val innerSubscriber: Subscriber[Any] = ObserverConverters.makeSafe(Subscriber(
    onNext = value => observer.onNext(value),
    onError = throwable => observer.onError(throwable),
    onCompleted = () => observer.onCompleted()
  ))

  override val subscriber: Subscriber[RxNotification[T]] = ObserverConverters.makeSafe(Subscriber(
    (rxNotification: RxNotification[T]) => rxNotification match {
      case RxOnNext(value) => innerSubscriber.onNext(value)
      case RxOnError(throwable) => innerSubscriber.onError(throwable)
      case RxOnCompleted() => innerSubscriber.onCompleted()
    },
    (throwable: Throwable) => innerSubscriber.onError(throwable),
    () => innerSubscriber.onCompleted()))
}
