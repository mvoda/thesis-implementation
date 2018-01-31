package rx.distributed.raft.statemachine.input

import rx.distributed.raft.consensus.server.Address.ServerAddress

case class DownstreamCommand(cluster: Set[ServerAddress], command: StatemachineCommand)
