package rx.distributed.raft.events.rpc.request

import rx.distributed.raft.events.consensus._
import rx.distributed.raft.events.rpc.response.{AppendEntriesResponse, VoteResponse}
import rx.lang.scala.Observer

sealed trait ConsensusRpcRequest extends RpcRequest[ConsensusRequest]

case class AppendEntriesRpcRequest(event: AppendEntries, responseObserver: Observer[AppendEntriesResponse]) extends ConsensusRpcRequest

case class VoteRequestRpcRequest(event: VoteRequest, responseObserver: Observer[VoteResponse]) extends ConsensusRpcRequest
