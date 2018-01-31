package rx.distributed.raft.util

import org.junit.Test
import org.scalatest.MustMatchers._
import rx.lang.scala.Observable
import rx.lang.scala.subjects.PublishSubject


class CommandQueueTest {
  private val clientCommands = Observable.just(IndexedCommand(1, null), IndexedCommand(2, null), IndexedCommand(3, null))

  @Test
  def replaysEvents(): Unit = {
    var sequenceNumbers = Seq[Int]()
    val obs = CommandQueue(clientCommands).commands
    obs.subscribe(v => sequenceNumbers = sequenceNumbers :+ v.index)
    obs.subscribe(v => sequenceNumbers = sequenceNumbers :+ v.index)
    sequenceNumbers.size mustBe 6
    sequenceNumbers mustBe Seq(1, 2, 3, 1, 2, 3)
  }

  @Test
  def replaysOnlyEventsWithoutResponse(): Unit = {
    var sequenceNumbers = Seq[Int]()
    val queue = CommandQueue(clientCommands)
    val obs = queue.commands
    obs.subscribe(v => sequenceNumbers = sequenceNumbers :+ v.index)
    queue.addResponseSeqNum(1)
    queue.addResponseSeqNum(3)
    obs.subscribe(v => sequenceNumbers = sequenceNumbers :+ v.index)
    sequenceNumbers.size mustBe 4
    sequenceNumbers mustBe Seq(1, 2, 3, 2)
  }

  @Test
  def completesWhenEmpty(): Unit = {
    var sequenceNumbers = Seq[Int]()
    val queue = CommandQueue(clientCommands)
    val obs = queue.commands
    obs.subscribe(v => sequenceNumbers = sequenceNumbers :+ v.index)
    queue.addResponseSeqNum(1)
    queue.addResponseSeqNum(3)
    queue.addResponseSeqNum(2)
    obs.subscribe(v => sequenceNumbers = sequenceNumbers :+ v.index)
    sequenceNumbers.size mustBe 3
    sequenceNumbers mustBe Seq(1, 2, 3)
  }

  @Test
  def correctlyQueuesAndReplaysNewEvents(): Unit = {
    var sequenceNumbers = Seq[Int]()
    val subject = PublishSubject[IndexedCommand]()
    val queue = CommandQueue(subject)
    val obs = queue.commands
    obs.subscribe(v => sequenceNumbers = sequenceNumbers :+ v.index)
    clientCommands.subscribe(v => subject.onNext(v))
    sequenceNumbers.size mustBe 3
    queue.addResponseSeqNum(1)
    queue.addResponseSeqNum(3)
    queue.addResponseSeqNum(2)
    sequenceNumbers.size mustBe 3
    subject.onNext(IndexedCommand(4, null))
    subject.onCompleted()
    sequenceNumbers.size mustBe 4
    sequenceNumbers mustBe Seq(1, 2, 3, 4)
    obs.subscribe(v => sequenceNumbers = sequenceNumbers :+ v.index)
    sequenceNumbers.size mustBe 5
    sequenceNumbers mustBe Seq(1, 2, 3, 4, 4)
  }
}
