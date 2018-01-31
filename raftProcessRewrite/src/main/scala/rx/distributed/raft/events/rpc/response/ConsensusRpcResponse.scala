package rx.distributed.raft.events.rpc.response

import rx.distributed.raft.events.consensus.ConsensusResponse

trait ConsensusRpcResponse extends RpcResponse with ConsensusResponse
