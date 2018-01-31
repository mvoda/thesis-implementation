package rx.distributed.raft.statemachine.output

import rx.distributed.raft.statemachine.input.DownstreamCommand

@SerialVersionUID(17L)
sealed trait StatemachineOutput extends Serializable

trait StatemachineResponse extends StatemachineOutput

trait ForwardingResponse extends StatemachineOutput {
  def response: StatemachineResponse

  def commands: Seq[DownstreamCommand]
}
