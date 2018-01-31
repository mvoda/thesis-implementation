package rx.distributed.raft.rpc.server

import io.grpc.stub.StreamObserver
import rx.distributed.raft._
import rx.distributed.raft.converters.{ObserverConverters, ProtobufConverters}
import rx.distributed.raft.events.rpc.request.{AppendEntriesRpcRequest, ConsensusRpcRequest, VoteRequestRpcRequest}
import rx.lang.scala.subjects.PublishSubject
import rx.lang.scala.{Observable, Subject}

case class ConsensusRPCServer() extends RaftGrpc.RaftImplBase with RPCServer[ConsensusRpcRequest] {
  private val eventSubject: Subject[ConsensusRpcRequest] = PublishSubject().toSerialized
  override val observable: Observable[ConsensusRpcRequest] = eventSubject

  override def appendEntries(request: AppendEntriesRequestProto, responseObserver: StreamObserver[AppendEntriesResponseProto]): Unit = {
    val event = ProtobufConverters.fromProtobuf(request)
    eventSubject.onNext(AppendEntriesRpcRequest(event, ObserverConverters.fromAppendEntriesStreamObserver(responseObserver)))
  }

  override def requestVote(request: VoteRequestProto, responseObserver: StreamObserver[VoteResponseProto]): Unit = {
    val event = ProtobufConverters.fromProtobuf(request)
    eventSubject.onNext(VoteRequestRpcRequest(event, ObserverConverters.fromVoteStreamObserver(responseObserver)))
  }
}
