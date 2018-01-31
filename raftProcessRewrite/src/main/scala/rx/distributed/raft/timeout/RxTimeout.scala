package rx.distributed.raft.timeout

import org.apache.logging.log4j.scala.Logging
import rx.distributed.raft.consensus.config.Config
import rx.lang.scala.schedulers.NewThreadScheduler
import rx.lang.scala.subjects.PublishSubject
import rx.lang.scala.{Observable, Observer, Scheduler, Subscription}

import scala.concurrent.duration._
import scala.util.Random

case class RxTimeout(min: Int, max: Int, scheduler: Scheduler = NewThreadScheduler()) extends Observable[Long] with Logging {
  private var sub: Subscription = Subscription()
  private val stream = PublishSubject[Long]()
  stream.subscribe(_ => reset())

  private def randomDelay(): Int = min + Random.nextInt(max - min + 1)

  def reset(): Unit = {
    this.synchronized {
      sub.unsubscribe()
      sub = Observable.timer(Duration(randomDelay(), MILLISECONDS), scheduler).doOnEach(v => stream.onNext(v)).subscribe()
    }
  }

  def start(): Unit = reset()

  def stop(): Unit = this.synchronized {
    sub.unsubscribe()
  }

  def onCompleted(): Unit = this.synchronized {
    stream.onCompleted()
  }

  override def subscribe(observer: Observer[Long]): Subscription = stream.subscribe(observer)

  override val asJavaObservable: rx.Observable[_ <: Long] = stream.asJavaObservable
}

object RxTimeout {
  def apply(time: Int): RxTimeout = RxTimeout(time, time)

  def apply(time: Int, scheduler: Scheduler): RxTimeout = RxTimeout(time, time, scheduler)

  def apply(config: Config): RxTimeout = apply(config.timeoutMin, config.timeoutMax)

  def apply(config: Config, scheduler: Scheduler): RxTimeout = apply(config.timeoutMin, config.timeoutMax, scheduler)

}
