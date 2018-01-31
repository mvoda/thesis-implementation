package rx.distributed.statemachine.responses

import rx.distributed.raft.consensus.server.Address.ServerAddress
import rx.distributed.raft.statemachine.input.{DownstreamCommand, StatemachineCommand}
import rx.distributed.raft.statemachine.output.{ForwardingResponse, StatemachineResponse}

case class ForwardingEmpty(commands: Seq[DownstreamCommand]) extends ForwardingResponse {
  override def response: StatemachineResponse = Empty()
}

object ForwardingEmpty {
  def apply(cluster: Set[ServerAddress], commands: Seq[StatemachineCommand]): ForwardingEmpty =
    ForwardingEmpty(commands.map(command => DownstreamCommand(cluster, command)))
}