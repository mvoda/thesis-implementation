package rx.distributed.raft.statemachine.output

import rx.distributed.raft.consensus.server.Address.ServerAddress
import rx.distributed.raft.statemachine.input.{DownstreamCommand, StatemachineCommand}

object StatemachineOutputUtils {

  case class EmptyCommand() extends StatemachineCommand

  case class AnotherCommand() extends StatemachineCommand

  case class EmptyResponse() extends StatemachineResponse

  case class AnotherResponse() extends StatemachineResponse

  def emptyDownstreamCommand(cluster: Set[ServerAddress]): DownstreamCommand = DownstreamCommand(cluster, EmptyCommand())

  case class ForwardingEmptyResponse(cluster: Set[ServerAddress]) extends ForwardingResponse {
    def response: StatemachineResponse = EmptyResponse()

    def commands: Seq[DownstreamCommand] = Seq(emptyDownstreamCommand(cluster))
  }

}
