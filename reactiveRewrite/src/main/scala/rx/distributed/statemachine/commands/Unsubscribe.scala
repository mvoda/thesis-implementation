package rx.distributed.statemachine.commands

import rx.distributed.raft.statemachine.input.StatemachineCommand

@SerialVersionUID(54L)
case class Unsubscribe(streamId: String) extends StatemachineCommand
