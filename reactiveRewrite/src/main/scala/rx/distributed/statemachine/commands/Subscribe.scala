package rx.distributed.statemachine.commands

import rx.distributed.observers.statemachine.StatemachineObserver
import rx.distributed.operators.Operator
import rx.distributed.raft.statemachine.input.StatemachineCommand

@SerialVersionUID(52L)
case class Subscribe(streamId: String, operators: Seq[Operator], observer: StatemachineObserver) extends StatemachineCommand
