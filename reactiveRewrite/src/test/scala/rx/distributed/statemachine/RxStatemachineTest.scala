package rx.distributed.statemachine

import org.apache.logging.log4j.scala.Logging
import org.junit.Test
import org.scalatest.MustMatchers._
import rx.distributed.observers.statemachine.LocalStatemachineObserver
import rx.distributed.operators.combining._
import rx.distributed.operators.errorHandling.OnErrorResumeNext
import rx.distributed.operators.single.{Retry, Take}
import rx.distributed.raft.consensus.server.Address.{ServerAddress, TestingAddress}
import rx.distributed.raft.statemachine.input.DownstreamCommand
import rx.distributed.statemachine.commands.{RxEvent, SetUpstreamForStreamId, Subscribe, Unsubscribe}
import rx.distributed.statemachine.responses.{Empty, ForwardingEmpty}
import rx.lang.scala.Notification.{OnCompleted, OnError, OnNext}

class RxStatemachineTest extends Logging {

  @Test
  def statemachineMerge(): Unit = {
    val leftStream = "mergeLeftStream"
    val rightStream = "mergeRightStream"
    val mergeOp = "mergeOperator"
    val statemachine = RxStatemachine(0)
    var responses = Seq[Any]()
    val localObserver = LocalStatemachineObserver(v => responses = responses :+ v)
    val subscribeLeft = Subscribe(leftStream, Seq(Merge(mergeOp, LeftStream())), localObserver)
    val subscribeRight = Subscribe(rightStream, Seq(Merge(mergeOp, RightStream())), localObserver)
    statemachine(subscribeLeft)
    statemachine(RxEvent(leftStream, OnNext(1)))
    statemachine(RxEvent(leftStream, OnNext(2)))
    statemachine(subscribeRight)
    statemachine.partialStreams mustBe Map()
    responses mustBe Seq(1, 2)
    statemachine(RxEvent(rightStream, OnNext(7)))
    responses mustBe Seq(1, 2, 7)
    statemachine(RxEvent(leftStream, OnNext(3)))
    responses mustBe Seq(1, 2, 7, 3)
    statemachine(RxEvent(leftStream, OnCompleted))
    statemachine.streams.get(leftStream) mustBe None
    statemachine(RxEvent(leftStream, OnNext(4)))
    responses mustBe Seq(1, 2, 7, 3)
    statemachine(RxEvent(rightStream, OnNext(8)))
    statemachine(RxEvent(rightStream, OnNext(9)))
    statemachine(RxEvent(rightStream, OnCompleted))
    responses mustBe Seq(1, 2, 7, 3, 8, 9)
    statemachine.streams.get(rightStream) mustBe None
  }

  @Test
  def statemachineDoubleMerge(): Unit = {
    val leftStream = "mergeLeftStream"
    val rightStream = "mergeRightStream"
    val thirdStream = "mergeThirdStream"
    val mergeOp = "mergeOperator"
    val mergeOp2 = "mergeOperator2"
    val statemachine = RxStatemachine(0)
    var responses = Seq[Any]()
    val localObserver = LocalStatemachineObserver(v => responses = responses :+ v)
    val subscribeLeft = Subscribe(leftStream, Seq(Merge(mergeOp, LeftStream()), Merge(mergeOp2, LeftStream())), localObserver)
    val subscribeRight = Subscribe(rightStream, Seq(Merge(mergeOp, RightStream()), Merge(mergeOp2, LeftStream())), localObserver)
    val subscribeThird = Subscribe(thirdStream, Seq(Merge(mergeOp2, RightStream())), localObserver)
    statemachine(subscribeLeft)
    statemachine(subscribeThird)
    statemachine(RxEvent(leftStream, OnNext(1)))
    statemachine(RxEvent(leftStream, OnNext(2)))
    statemachine(RxEvent(thirdStream, OnNext(10)))
    statemachine(subscribeRight)
    statemachine.partialStreams mustBe Map()
    responses mustBe Seq(1, 2, 10)
    statemachine(RxEvent(rightStream, OnNext(7)))
    responses mustBe Seq(1, 2, 10, 7)
    statemachine(RxEvent(thirdStream, OnNext(11)))
    statemachine(RxEvent(leftStream, OnNext(3)))
    responses mustBe Seq(1, 2, 10, 7, 11, 3)
    statemachine(RxEvent(leftStream, OnCompleted))
    statemachine.streams.get(leftStream) mustBe None
    statemachine(RxEvent(thirdStream, OnCompleted))
    statemachine.streams.get(thirdStream) mustBe None
    statemachine(RxEvent(leftStream, OnNext(4)))
    responses mustBe Seq(1, 2, 10, 7, 11, 3)
    statemachine(RxEvent(rightStream, OnNext(8)))
    statemachine(RxEvent(rightStream, OnNext(9)))
    statemachine(RxEvent(rightStream, OnCompleted))
    responses mustBe Seq(1, 2, 10, 7, 11, 3, 8, 9)
    statemachine.streams.get(rightStream) mustBe None
  }

  @Test
  def statemachineConcat(): Unit = {
    val leftStream = "concatLeftStream"
    val rightStream = "concatRightStream"
    val concatOp = "concatOperator"
    val statemachine = RxStatemachine(0)
    var responses = Seq[Any]()
    val localObserver = LocalStatemachineObserver(v => responses = responses :+ v)
    val subscribeLeft = Subscribe(leftStream, Seq(Concat(concatOp, LeftStream())), localObserver)
    val subscribeRight = Subscribe(rightStream, Seq(Concat(concatOp, RightStream())), localObserver)
    statemachine(subscribeLeft)
    statemachine(subscribeRight)
    statemachine.partialStreams mustBe Map()
    statemachine(RxEvent(leftStream, OnNext(1)))
    statemachine(RxEvent(leftStream, OnNext(2)))
    responses mustBe Seq(1, 2)
    statemachine(RxEvent(rightStream, OnNext(7)))
    responses mustBe Seq(1, 2)
    statemachine(RxEvent(leftStream, OnNext(3)))
    responses mustBe Seq(1, 2, 3)
    statemachine(RxEvent(leftStream, OnCompleted))
    responses mustBe Seq(1, 2, 3)
    statemachine.streams.get(leftStream) mustBe None
    statemachine(RxEvent(leftStream, OnNext(4)))
    responses mustBe Seq(1, 2, 3)
    statemachine(RxEvent(leftStream, OnCompleted))
    statemachine(RxEvent(leftStream, OnNext(50)))
    statemachine(RxEvent(rightStream, OnNext(8)))
    statemachine(RxEvent(rightStream, OnNext(9)))
    statemachine(RxEvent(rightStream, OnCompleted))
    statemachine(RxEvent(rightStream, OnNext(50)))
    responses mustBe Seq(1, 2, 3, 8, 9)
    statemachine.streams.get(rightStream) mustBe None
  }

  @Test
  def statemachineOnErrorResumeNext(): Unit = {
    val stream = "initial"
    val errorRecoveryStream = "errorRecovery"
    val statemachine = RxStatemachine(0)
    var responses = Seq[Any]()
    val localObserver = LocalStatemachineObserver(v => responses = responses :+ v)
    val subscribe = Subscribe(stream, Seq(OnErrorResumeNext(_ => (errorRecoveryStream, Seq(Take(2))))), localObserver)
    statemachine(subscribe)
    statemachine(RxEvent(stream, OnNext(1)))
    statemachine(RxEvent(stream, OnNext(2)))
    responses mustBe Seq(1, 2)
    statemachine(RxEvent(stream, OnError(new Exception())))
    responses mustBe Seq(1, 2)
    statemachine.streams.get(stream) mustBe None
    statemachine(RxEvent(stream, OnNext(3)))
    responses mustBe Seq(1, 2)
    statemachine(RxEvent(errorRecoveryStream, OnNext(3)))
    responses mustBe Seq(1, 2, 3)
    statemachine(RxEvent(errorRecoveryStream, OnNext(4)))
    responses mustBe Seq(1, 2, 3, 4)
    statemachine(RxEvent(errorRecoveryStream, OnNext(8)))
    responses mustBe Seq(1, 2, 3, 4)
    statemachine.streams.get(errorRecoveryStream) mustBe None
  }

  @Test
  def takeUntil(): Unit = {
    val leftStream = "leftStream"
    val rightStream = "rightStream"
    val takeUntilOp = "takeUntil"
    val statemachine = RxStatemachine(0)
    var responses = Seq[Any]()
    val localObserver = LocalStatemachineObserver(v => responses = responses :+ v)
    val subscribeLeft = Subscribe(leftStream, Seq(TakeUntil(takeUntilOp, LeftStream())), localObserver)
    val subscribeRight = Subscribe(rightStream, Seq(TakeUntil(takeUntilOp, RightStream())), localObserver)
    statemachine(subscribeRight)
    statemachine(subscribeLeft)
    statemachine.partialStreams mustBe Map()
    statemachine(RxEvent(leftStream, OnNext(1)))
    statemachine(RxEvent(leftStream, OnNext(2)))
    responses mustBe Seq(1, 2)
    statemachine(RxEvent(rightStream, OnNext(7)))
    responses mustBe Seq(1, 2)
    statemachine.streams.get(leftStream) mustBe None
    statemachine.streams.get(rightStream) mustBe None
  }

  @Test
  def statemachineRetry(): Unit = {
    val stream = "initial"
    val statemachine = RxStatemachine(0)
    var responses = Seq[Any]()
    val localObserver = LocalStatemachineObserver(v => responses = responses :+ v)
    val subscribe = Subscribe(stream, Seq(Retry()), localObserver)
    statemachine(subscribe)
    statemachine(RxEvent(stream, OnNext(1)))
    statemachine(RxEvent(stream, OnNext(2)))
    responses mustBe Seq(1, 2)
    statemachine(RxEvent(stream, OnError(new Exception())))
    responses mustBe Seq(1, 2)
    statemachine.streams(stream).isUnsubscribed mustBe false
    statemachine(RxEvent(stream, OnNext(3)))
    responses mustBe Seq(1, 2, 3)
    statemachine(RxEvent(stream, OnCompleted))
    statemachine.streams.get(stream) mustBe None
  }

  @Test
  def statemachineRetryDetectsUnsubscribe(): Unit = {
    val stream = "initial"
    val statemachine = RxStatemachine(0)
    var responses = Seq[Any]()
    val localObserver = LocalStatemachineObserver(v => responses = responses :+ v)
    val subscribe = Subscribe(stream, Seq(Retry()), localObserver)
    statemachine(subscribe)
    statemachine(RxEvent(stream, OnNext(1)))
    statemachine(RxEvent(stream, OnNext(2)))
    responses mustBe Seq(1, 2)
    statemachine(RxEvent(stream, OnError(new Exception())))
    responses mustBe Seq(1, 2)
    statemachine.streams(stream).isUnsubscribed mustBe false
    statemachine(Unsubscribe(stream))
    responses mustBe Seq(1, 2)
    statemachine.streams mustBe Map()
  }

  @Test
  def setUpstreamArrivesAfterCompleted(): Unit = {
    val upstreamCluster = Set(TestingAddress("upstream").asInstanceOf[ServerAddress])
    val upstreamStreamId = "upstreamStreamId"
    val stream = "initial"
    val statemachine = RxStatemachine(0)
    var responses = Seq[Any]()
    val localObserver = LocalStatemachineObserver(v => responses = responses :+ v)
    val subscribe = Subscribe(stream, Seq(), localObserver)
    statemachine(subscribe)
    val output = statemachine(RxEvent(stream, OnCompleted))
    output mustBe Empty()
    val setUpstreamOutput = statemachine(SetUpstreamForStreamId(stream, upstreamCluster, upstreamStreamId))
    setUpstreamOutput mustBe an[ForwardingEmpty]
    setUpstreamOutput.asInstanceOf[ForwardingEmpty].commands.size mustBe 1
    setUpstreamOutput.asInstanceOf[ForwardingEmpty].commands.head mustBe DownstreamCommand(upstreamCluster, Unsubscribe(upstreamStreamId))
  }

  @Test
  def setUpstreamArrivesAfterUnsubscribe(): Unit = {
    val upstreamCluster = Set(TestingAddress("upstream").asInstanceOf[ServerAddress])
    val upstreamStreamId = "upstreamStreamId"
    val stream = "initial"
    val statemachine = RxStatemachine(0)
    var responses = Seq[Any]()
    val localObserver = LocalStatemachineObserver(v => responses = responses :+ v)
    val subscribe = Subscribe(stream, Seq(), localObserver)
    statemachine(subscribe)
    val output = statemachine(Unsubscribe(stream))
    output mustBe Empty()
    val setUpstreamOutput = statemachine(SetUpstreamForStreamId(stream, upstreamCluster, upstreamStreamId))
    setUpstreamOutput mustBe an[ForwardingEmpty]
    setUpstreamOutput.asInstanceOf[ForwardingEmpty].commands.size mustBe 1
    setUpstreamOutput.asInstanceOf[ForwardingEmpty].commands.head mustBe DownstreamCommand(upstreamCluster, Unsubscribe(upstreamStreamId))
  }
}
