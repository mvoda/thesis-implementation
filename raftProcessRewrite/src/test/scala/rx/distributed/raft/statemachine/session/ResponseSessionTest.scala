package rx.distributed.raft.statemachine.session

import org.junit.Test
import org.scalatest.MustMatchers._
import rx.distributed.raft.statemachine.output.StatemachineOutputUtils.{AnotherResponse, EmptyResponse}

import scala.collection.immutable.SortedMap


class ResponseSessionTest {
  private val TIMESTAMP = 1000
  private val EMPTY_SESSION = ResponseSession(None, SortedMap(), 0, TIMESTAMP)

  @Test
  def addsNewResponse(): Unit = {
    val session = EMPTY_SESSION.update(1, 0, EmptyResponse(), TIMESTAMP + 1)
    session.timestamp mustBe TIMESTAMP + 1
    session.isDiscarded(1) mustBe false
    session.getResponse(1) mustBe EmptyResponse()
    session.saved(1) mustBe true
  }

  @Test
  def addsTwoNewResponses(): Unit = {
    val session = EMPTY_SESSION.update(1, 0, EmptyResponse(), TIMESTAMP + 1).update(2, 0, AnotherResponse(), TIMESTAMP + 2)
    session.timestamp mustBe TIMESTAMP + 2
    session.isDiscarded(1) mustBe false
    session.getResponse(1) mustBe EmptyResponse()
    session.saved(1) mustBe true
    session.isDiscarded(2) mustBe false
    session.getResponse(2) mustBe AnotherResponse()
    session.saved(2) mustBe true
  }

  @Test
  def correctlyDiscardsOnUpdate(): Unit = {
    val session = EMPTY_SESSION.update(1, 0, EmptyResponse(), TIMESTAMP + 1).update(2, 2, AnotherResponse(), TIMESTAMP + 2)
    session.timestamp mustBe TIMESTAMP + 2
    session.isDiscarded(1) mustBe true
    session.saved(1) mustBe false
    session.isDiscarded(2) mustBe false
    session.getResponse(2) mustBe AnotherResponse()
    session.saved(2) mustBe true
  }

  @Test
  def correctlyDiscardsOnKeepAlive(): Unit = {
    val session = EMPTY_SESSION.update(1, 0, EmptyResponse(), TIMESTAMP + 1).keepAlive(2, TIMESTAMP + 2)
    session.timestamp mustBe TIMESTAMP + 2
    session.isDiscarded(1) mustBe true
    session.saved(1) mustBe false
  }

  @Test
  def keepAliveUpdatesTimestamp(): Unit = {
    val session = EMPTY_SESSION.update(1, 1, EmptyResponse(), TIMESTAMP + 1).keepAlive(1, TIMESTAMP + TIMESTAMP)
    session.timestamp mustBe TIMESTAMP + TIMESTAMP
    session.isDiscarded(1) mustBe false
    session.getResponse(1) mustBe EmptyResponse()
    session.saved(1) mustBe true
  }
}
