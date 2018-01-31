package rx.distributed.statemachine

import rx.distributed.notifications.RxNotification
import rx.lang.scala.subjects.PublishSubject
import rx.lang.scala.{Observable, Observer, Subject, Subscription}

case class StreamInput[T](private val subscription: Subscription) {
  private var ownSubscription: Subscription = initSubscription()
  private val subject: Subject[RxNotification[T]] = PublishSubject[RxNotification[T]]()
  val observable: Observable[T] = subject.doOnUnsubscribe(unsubscribe()).doOnSubscribe(subscribe()).map(_.toNotification).dematerialize
  val observer: Observer[RxNotification[T]] = subject

  def isUnsubscribed: Boolean = subscription.isUnsubscribed || ownSubscription.isUnsubscribed

  private def initSubscription(): Subscription = {
    val subscription = Subscription()
    subscription.unsubscribe()
    subscription
  }

  private def subscribe(): Unit = ownSubscription = Subscription()

  private def unsubscribe(): Unit = ownSubscription.unsubscribe()

}
