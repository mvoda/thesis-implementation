package rx.distributed.statemachine.responses

import rx.distributed.raft.statemachine.output.StatemachineResponse

case class Empty() extends StatemachineResponse
