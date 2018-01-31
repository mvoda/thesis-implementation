package rx.distributed.raft.converters

import org.apache.logging.log4j.scala.Logging
import org.junit.Test
import org.scalatest.MustMatchers._
import rx.lang.scala.Subscriber
import rx.lang.scala.subjects.PublishSubject

class ObserverConvertersTest extends Logging {
  @Test
  def convertingSubjectToStreamObserverRespectsContract(): Unit = {
    val subject = PublishSubject[Int]()
    var events = Seq[Int]()
    subject.subscribe(v => events = events :+ v, e => logger.error(e))
    val commandObserver = ObserverConverters.fromRx(subject)
    commandObserver.onNext(1)
    commandObserver.onNext(2)
    commandObserver.onError(new Exception())
    commandObserver.onCompleted()
    commandObserver.onNext(3)
    events.size mustBe 2
    events.head mustBe 1
    events(1) mustBe 2
  }

  @Test
  def convertingSubscriberToStreamObserverRespectsContract(): Unit = {
    var events = Seq[Int]()
    val observer = Subscriber((v: Int) => events = events :+ v, e => logger.error(e))
    val commandObserver = ObserverConverters.fromRx(observer)
    commandObserver.onNext(1)
    commandObserver.onNext(2)
    commandObserver.onError(new Exception())
    commandObserver.onCompleted()
    commandObserver.onNext(3)
    events.size mustBe 2
    events.head mustBe 1
    events(1) mustBe 2
  }
}
