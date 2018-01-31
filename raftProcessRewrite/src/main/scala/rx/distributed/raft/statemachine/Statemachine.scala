package rx.distributed.raft.statemachine

import rx.distributed.raft.statemachine.input.StatemachineCommand
import rx.distributed.raft.statemachine.output.StatemachineOutput

trait Statemachine {
  // make sure lastApplied is as durable as the state machine
  final var lastApplied: Long = 0

  def sessionTimeout: Int

  final def apply(command: StatemachineCommand, index: Long): StatemachineOutput = {
    lastApplied = index
    apply(command)
  }

  def apply(command: StatemachineCommand): StatemachineOutput
}