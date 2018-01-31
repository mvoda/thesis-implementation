package rx.distributed.statemachine.responses

import rx.distributed.raft.statemachine.input.DownstreamCommand
import rx.distributed.raft.statemachine.output.{ForwardingResponse, StatemachineResponse}

case class Forwarding(response: StatemachineResponse, commands: Seq[DownstreamCommand]) extends ForwardingResponse
