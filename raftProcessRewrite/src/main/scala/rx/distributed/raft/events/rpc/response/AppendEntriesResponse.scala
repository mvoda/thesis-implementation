package rx.distributed.raft.events.rpc.response

import rx.distributed.raft.AppendEntriesResponseProto
import rx.distributed.raft.consensus.server.Address
import rx.distributed.raft.consensus.server.Address.ServerAddress
import rx.distributed.raft.events.internal.{Accept, Reject, Response}

sealed trait AppendEntriesResponse extends ConsensusRpcResponse {
  def toProtobuf: AppendEntriesResponseProto
}

final case class AppendEntriesSuccess(term: Int, address: ServerAddress, matchIndex: Int) extends AppendEntriesResponse {
  def toProtobuf: AppendEntriesResponseProto =
    AppendEntriesResponseProto.newBuilder().setTerm(term).setAddress(address.toProtobuf).setSuccess(true).setMatchIndex(matchIndex).build()
}

final case class AppendEntriesFailure(term: Int, address: ServerAddress) extends AppendEntriesResponse {
  def toProtobuf: AppendEntriesResponseProto =
    AppendEntriesResponseProto.newBuilder().setTerm(term).setAddress(address.toProtobuf).setSuccess(false).build()
}

object AppendEntriesResponse {
  def apply(response: AppendEntriesResponseProto): AppendEntriesResponse = response match {
    case successResponse if response.getSuccess =>
      AppendEntriesSuccess(successResponse.getTerm, Address(successResponse.getAddress), successResponse.getMatchIndex)
    case failureResponse =>
      AppendEntriesFailure(failureResponse.getTerm, Address(failureResponse.getAddress))
  }

  def apply(response: Response, term: Int, address: ServerAddress, lastIndex: Int): AppendEntriesResponse = response match {
    case Accept() => AppendEntriesSuccess(term, address, lastIndex)
    case Reject() => AppendEntriesFailure(term, address)
  }
}
