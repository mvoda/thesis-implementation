package rx.distributed.raft

import java.util.concurrent.CountDownLatch

import org.apache.logging.log4j.scala.Logging
import rx.distributed.raft.converters.ObserverConverters
import rx.lang.scala.{Observer, Subscriber}

object LatchObservers extends Logging {

  trait LatchObserver[T] {
    def events: Seq[T]

    def errors: Seq[Throwable]

    def latch: CountDownLatch

    def observer: Observer[T]
  }

  case class OnCompletedCountdown[T](count: Int, onNextAction: T => Unit = (v: T) => {}) extends LatchObserver[T] {
    var events: Seq[T] = Seq()
    var errors: Seq[Throwable] = Seq()
    val latch: CountDownLatch = new CountDownLatch(count)
    val observer: Observer[T] = ObserverConverters.makeSafe(Subscriber[T](
      (event: T) => {
        events = events :+ event
        onNextAction(event)
      },
      (e: Throwable) => {
        logger.warn(s"OnCompletedCountdown($count) received error: $e")
        errors = errors :+ e
        latch.countDown()
      },
      () => latch.countDown()))
  }

  case class OnNextCountdown[T](count: Int) extends LatchObserver[T] {
    var events: Seq[T] = Seq()
    var errors: Seq[Throwable] = Seq()
    val latch: CountDownLatch = new CountDownLatch(count)
    val observer: Observer[T] = ObserverConverters.makeSafe(Subscriber[T](
      (event: T) => {
        events = events :+ event
        latch.countDown()
      },
      (e: Throwable) => {
        logger.warn(s"OnNextCountdown($count) received error: $e")
        errors = errors :+ e
        latch.countDown()
      }))
  }

}
