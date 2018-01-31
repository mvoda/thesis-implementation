package rx.distributed.raft.util

import org.junit.Test
import org.scalatest.MustMatchers._
import rx.distributed.raft.statemachine.input.StatemachineCommand

class ByteStringDeserializerTest {
  @Test
  def deserializesNullCommand(): Unit = {
    val indexedCommand = IndexedCommand(1, null)
    val serializedCommand = indexedCommand.toProtobuf("clientId", 0)
    val deserializedCommand = ByteStringDeserializer.deserialize[StatemachineCommand](serializedCommand.getCommand)
    indexedCommand.command mustBe deserializedCommand
  }

  @Test
  def deserializesTestCommand(): Unit = {
    val indexedCommand = IndexedCommand(1, TestCommand(1337))
    val serializedCommand = indexedCommand.toProtobuf("clientId", 0)
    val deserializedCommand = ByteStringDeserializer.deserialize[StatemachineCommand](serializedCommand.getCommand)
    indexedCommand.command mustBe deserializedCommand
  }
}

case class TestCommand(testValue: Int) extends StatemachineCommand
