package rx.distributed.statemachine.commands

import rx.distributed.raft.consensus.server.Address.ServerAddress
import rx.distributed.raft.statemachine.input.StatemachineCommand

@SerialVersionUID(55L)
case class SetUpstreamForStreamId(streamId: String, upstreamCluster: Set[ServerAddress], upstreamStreamId: String) extends StatemachineCommand
