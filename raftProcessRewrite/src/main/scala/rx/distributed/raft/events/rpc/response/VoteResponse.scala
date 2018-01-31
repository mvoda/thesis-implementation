package rx.distributed.raft.events.rpc.response

import rx.distributed.raft.VoteResponseProto
import rx.distributed.raft.consensus.server.Address
import rx.distributed.raft.consensus.server.Address.ServerAddress
import rx.distributed.raft.events.internal.{Accept, Reject, Response}

sealed trait VoteResponse extends ConsensusRpcResponse {
  def toProtobuf: VoteResponseProto
}

final case class VoteGranted(term: Int, address: ServerAddress) extends VoteResponse {
  def toProtobuf: VoteResponseProto =
    VoteResponseProto.newBuilder().setTerm(term).setAddress(address.toProtobuf).setVoteGranted(true).build()
}

final case class VoteRejected(term: Int, address: ServerAddress) extends VoteResponse {
  def toProtobuf: VoteResponseProto =
    VoteResponseProto.newBuilder().setTerm(term).setAddress(address.toProtobuf).setVoteGranted(false).build()
}

object VoteResponse {
  def apply(response: VoteResponseProto): VoteResponse = response match {
    case voteGranted if response.getVoteGranted => VoteGranted(voteGranted.getTerm, Address(voteGranted.getAddress))
    case voteRejected => VoteRejected(voteRejected.getTerm, Address(voteRejected.getAddress))
  }

  def apply(response: Response, term: Int, address: ServerAddress): VoteResponse = response match {
    case Accept() => VoteGranted(term, address)
    case Reject() => VoteRejected(term, address)
  }
}