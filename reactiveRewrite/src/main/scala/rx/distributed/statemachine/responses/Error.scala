package rx.distributed.statemachine.responses

import rx.distributed.raft.statemachine.output.StatemachineResponse

case class Error(e: Throwable) extends StatemachineResponse
