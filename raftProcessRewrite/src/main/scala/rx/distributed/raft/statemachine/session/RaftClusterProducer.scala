package rx.distributed.raft.statemachine.session

import rx.distributed.raft.consensus.server.Address.ServerAddress

case class RaftClusterProducer(cluster: Set[ServerAddress], producer: ClientProducer)
