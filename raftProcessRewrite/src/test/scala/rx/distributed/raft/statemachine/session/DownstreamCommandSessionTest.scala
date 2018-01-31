package rx.distributed.raft.statemachine.session

import org.junit.Test
import org.scalatest.MustMatchers._
import rx.distributed.raft.statemachine.output.StatemachineOutputUtils.{AnotherCommand, EmptyCommand}

class DownstreamCommandSessionTest {
  private val INIT_SESSION = DownstreamCommandSession(producerExpired = false, None, 1, Seq(EmptyCommand()))

  @Test
  def removesCommand(): Unit = {
    val session = INIT_SESSION.removeCommands(2)
    session.commands.size mustBe 0
    session.startingSeqNum mustBe 2
    session.producerExpired mustBe false
    session.isCompleted mustBe false
  }

  @Test
  def addsCommand(): Unit = {
    val session = INIT_SESSION.addCommand(AnotherCommand())
    session.commands.size mustBe 2
    session.startingSeqNum mustBe 1
    session.producerExpired mustBe false
    session.isCompleted mustBe false
  }

  @Test
  def removesCommandWithSameSeqNumIsNoOp(): Unit = {
    val session = INIT_SESSION.addCommand(AnotherCommand()).removeCommands(2).removeCommands(2)
    session.commands.size mustBe 1
    session.startingSeqNum mustBe 2
    session.producerExpired mustBe false
    session.isCompleted mustBe false
  }

  @Test
  def expiresProducers(): Unit = {
    val session = INIT_SESSION.addCommand(AnotherCommand()).expire()
    session.commands.size mustBe 2
    session.startingSeqNum mustBe 1
    session.producerExpired mustBe true
    session.isCompleted mustBe false
  }

  @Test
  def completesWhenCommandsDeliveredAndProducerExpired(): Unit = {
    val session = INIT_SESSION.addCommand(AnotherCommand()).removeCommands(2).expire()
    session.commands.size mustBe 1
    session.startingSeqNum mustBe 2
    session.producerExpired mustBe true
    session.isCompleted mustBe false
    val newSesison = session.removeCommands(3)
    newSesison.commands.size mustBe 0
    newSesison.startingSeqNum mustBe 3
    newSesison.producerExpired mustBe true
    newSesison.isCompleted mustBe true
  }
}
