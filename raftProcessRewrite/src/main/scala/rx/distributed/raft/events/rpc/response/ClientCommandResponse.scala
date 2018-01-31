package rx.distributed.raft.events.rpc.response

import com.google.protobuf.ByteString
import rx.distributed.raft.ClientResponseProto
import rx.distributed.raft.consensus.server.Address
import rx.distributed.raft.consensus.server.Address.ServerAddress

trait ClientCommandResponse extends RpcResponse {
  override def toProtobuf: ClientResponseProto
}

case class ClientResponseSuccessful(seqNum: Int, statemachineResponse: ByteString) extends ClientCommandResponse {
  override def toProtobuf: ClientResponseProto =
    ClientResponseProto.newBuilder().setStatus(ClientResponseProto.Status.OK).setResponse(statemachineResponse).setCommandSeqNum(seqNum).build()
}

case class ClientResponseNotLeader(leaderHint: Option[ServerAddress]) extends ClientCommandResponse with NotLeaderRpcResponse {
  override def toProtobuf: ClientResponseProto = {
    val response = ClientResponseProto.newBuilder().setStatus(ClientResponseProto.Status.NOT_LEADER)
    leaderHint.foreach(address => response.setLeaderHint(address.toProtobuf))
    response.build()
  }
}

case class ClientResponseSessionExpired() extends ClientCommandResponse {
  override def toProtobuf: ClientResponseProto = ClientResponseProto.newBuilder().setStatus(ClientResponseProto.Status.SESSION_EXPIRED).build()
}

//TODO: re-add when implementing client queries
//case class ClientResponseInvalidQuery() extends ClientCommandResponse {
//  override def toProtobuf: ClientResponseProto = ClientResponseProto.newBuilder().setStatus(ClientResponseProto.Status.INVALID_QUERY).build()
//}

object ClientCommandResponse {
  def apply(proto: ClientResponseProto): ClientCommandResponse = proto.getStatus match {
    case ClientResponseProto.Status.SESSION_EXPIRED => ClientResponseSessionExpired()
    case ClientResponseProto.Status.OK => ClientResponseSuccessful(proto.getCommandSeqNum, proto.getResponse)
    case ClientResponseProto.Status.NOT_LEADER =>
      ClientResponseNotLeader(Option(proto.hasLeaderHint).collect { case true => proto.getLeaderHint }.map(addressProto => Address(addressProto)))
  }
}