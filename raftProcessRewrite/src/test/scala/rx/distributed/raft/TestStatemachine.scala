package rx.distributed.raft

import org.apache.logging.log4j.scala.Logging
import rx.distributed.raft.consensus.server.Address.ServerAddress
import rx.distributed.raft.statemachine.Statemachine
import rx.distributed.raft.statemachine.input.{DownstreamCommand, StatemachineCommand}
import rx.distributed.raft.statemachine.output.{ForwardingResponse, StatemachineOutput, StatemachineResponse}

case class TestStatemachine(sessionTimeout: Int = 30000) extends Statemachine with Logging {
  var map: Map[Int, Int] = Map()

  override def apply(command: StatemachineCommand): StatemachineOutput = {
    logger.info(s"Statemachine received command: $command")
    command match {
      case Write(key, value) =>
        map = map + (key -> value)
        Value(value)
      case DownstreamWrite(key, value, cluster) => {
        map = map + (key -> value)
        DownstreamResponse(DownstreamCommand(cluster, Write(key, value * 10)))
      }
      case Read(key) => Value(map(key))
    }
  }
}

case class Read(key: Int) extends StatemachineCommand

case class Write(key: Int, value: Int) extends StatemachineCommand

case class DownstreamWrite(key: Int, value: Int, cluster: Set[ServerAddress]) extends StatemachineCommand

case class Empty() extends StatemachineResponse

case class Value(value: Int) extends StatemachineResponse

case class DownstreamResponse(command: DownstreamCommand) extends ForwardingResponse {
  override def response: StatemachineResponse = Empty()

  override def commands: Seq[DownstreamCommand] = Seq(command)
}
