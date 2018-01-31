package rx.distributed.raft.events.rpc.response

import rx.distributed.raft.consensus.server.Address.ServerAddress

trait NotLeaderRpcResponse extends RpcResponse {
  def leaderHint: Option[ServerAddress]
}
