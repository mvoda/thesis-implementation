package rx.distributed.statemachine.responses

import rx.distributed.raft.statemachine.input.StatemachineCommand
import rx.distributed.raft.statemachine.output.StatemachineResponse

case class Unsubscribed(streamId: String) extends StatemachineResponse with StatemachineCommand
