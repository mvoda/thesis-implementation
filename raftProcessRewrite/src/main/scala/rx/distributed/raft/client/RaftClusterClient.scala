package rx.distributed.raft.client

import rx.distributed.raft.consensus.server.Address.ServerAddress

case class RaftClusterClient(cluster: Set[ServerAddress], id: String)
