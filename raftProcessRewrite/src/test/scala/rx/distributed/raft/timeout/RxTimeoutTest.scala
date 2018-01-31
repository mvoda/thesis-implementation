package rx.distributed.raft.timeout

import org.junit.Test
import org.scalatest.MustMatchers._
import rx.lang.scala.schedulers.TestScheduler

import scala.concurrent.duration.{Duration, MILLISECONDS}

class RxTimeoutTest {
  @Test
  def firesAfterMaxDelay(): Unit = {
    val testScheduler = TestScheduler()
    val rxTimeout = RxTimeout(51, 100, testScheduler)
    var events = Seq[Long]()
    rxTimeout.subscribe(v => events = events :+ v)
    rxTimeout.start()
    testScheduler.advanceTimeBy(Duration(100, MILLISECONDS))
    events.size mustBe 1
  }

  @Test
  def firesMultipleTimes(): Unit = {
    val testScheduler = TestScheduler()
    val rxTimeout = RxTimeout(50, 50, testScheduler)
    var events = Seq[Long]()
    rxTimeout.subscribe(v => events = events :+ v)
    rxTimeout.start()
    testScheduler.advanceTimeBy(Duration(100, MILLISECONDS))
    events.size mustBe 2
  }

  @Test
  def doesNotFireIfReset(): Unit = {
    val testScheduler = TestScheduler()
    val rxTimeout = RxTimeout(50, 100, testScheduler)
    var events = Seq[Long]()
    rxTimeout.subscribe(v => events = events :+ v)
    rxTimeout.start()
    testScheduler.advanceTimeBy(Duration(49, MILLISECONDS))
    rxTimeout.reset()
    events.size mustBe 0
  }

  @Test
  def firesAfterReset(): Unit = {
    val testScheduler = TestScheduler()
    val rxTimeout = RxTimeout(51, 100, testScheduler)
    var events = Seq[Long]()
    rxTimeout.subscribe(v => events = events :+ v)
    rxTimeout.start()
    testScheduler.advanceTimeBy(Duration(50, MILLISECONDS))
    rxTimeout.reset()
    testScheduler.advanceTimeBy(Duration(100, MILLISECONDS))
    events.size mustBe 1
  }

  @Test
  def doesNotFireIfMultipleReset(): Unit = {
    val testScheduler = TestScheduler()
    val rxTimeout = RxTimeout(50, 100, testScheduler)
    var events = Seq[Long]()
    rxTimeout.subscribe(v => events = events :+ v)
    rxTimeout.start()
    testScheduler.advanceTimeBy(Duration(49, MILLISECONDS))
    rxTimeout.reset()
    testScheduler.advanceTimeBy(Duration(49, MILLISECONDS))
    rxTimeout.reset()
    events.size mustBe 0
  }

  @Test
  def doesNotFireIfStopped(): Unit = {
    val testScheduler = TestScheduler()
    val rxTimeout = RxTimeout(50, 100, testScheduler)
    var events = Seq[Long]()
    rxTimeout.subscribe(v => events = events :+ v)
    rxTimeout.start()
    testScheduler.advanceTimeBy(Duration(49, MILLISECONDS))
    rxTimeout.stop()
    testScheduler.advanceTimeBy(Duration(150, MILLISECONDS))
    events.size mustBe 0
  }

  @Test
  def firesAfterStoppedAndStarted(): Unit = {
    val testScheduler = TestScheduler()
    val rxTimeout = RxTimeout(51, 100, testScheduler)
    var events = Seq[Long]()
    rxTimeout.subscribe(v => events = events :+ v)
    rxTimeout.start()
    testScheduler.advanceTimeBy(Duration(50, MILLISECONDS))
    rxTimeout.stop()
    testScheduler.advanceTimeBy(Duration(150, MILLISECONDS))
    events.size mustBe 0
    rxTimeout.start()
    testScheduler.advanceTimeBy(Duration(100, MILLISECONDS))
    events.size mustBe 1
  }

  @Test
  def doesNotFireAfterOnCompleted(): Unit = {
    val testScheduler = TestScheduler()
    val rxTimeout = RxTimeout(50, 100, testScheduler)
    var events = Seq[Long]()
    rxTimeout.subscribe(v => events = events :+ v)
    rxTimeout.start()
    testScheduler.advanceTimeBy(Duration(49, MILLISECONDS))
    rxTimeout.onCompleted()
    testScheduler.advanceTimeBy(Duration(150, MILLISECONDS))
    events.size mustBe 0
    rxTimeout.start()
    testScheduler.advanceTimeBy(Duration(150, MILLISECONDS))
    events.size mustBe 0
  }
}
