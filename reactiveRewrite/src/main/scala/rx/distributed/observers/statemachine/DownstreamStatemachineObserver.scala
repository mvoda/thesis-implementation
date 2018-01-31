package rx.distributed.observers.statemachine

import rx.distributed.raft.consensus.server.Address.ServerAddress

case class DownstreamStatemachineObserver(streamId: String, cluster: Set[ServerAddress]) extends StatemachineObserver
