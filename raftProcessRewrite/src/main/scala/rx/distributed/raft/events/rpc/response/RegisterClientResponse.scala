package rx.distributed.raft.events.rpc.response

import rx.distributed.raft.RegisterClientResponseProto
import rx.distributed.raft.consensus.server.Address
import rx.distributed.raft.consensus.server.Address.ServerAddress


trait RegisterClientResponse extends RpcResponse {
  override def toProtobuf: RegisterClientResponseProto
}

case class RegisterClientSuccessful(clientId: String) extends RegisterClientResponse {
  override def toProtobuf: RegisterClientResponseProto = {
    RegisterClientResponseProto.newBuilder().setStatus(RegisterClientResponseProto.Status.OK).setClientId(clientId).build()
  }
}

case class RegisterClientNotLeader(leaderHint: Option[ServerAddress]) extends RegisterClientResponse with NotLeaderRpcResponse {
  override def toProtobuf: RegisterClientResponseProto = {
    val response = RegisterClientResponseProto.newBuilder().setStatus(RegisterClientResponseProto.Status.NOT_LEADER)
    leaderHint.foreach(address => response.setLeaderHint(address.toProtobuf))
    response.build()
  }
}

object RegisterClientResponse {
  def apply(proto: RegisterClientResponseProto): RegisterClientResponse = proto.getStatus match {
    case RegisterClientResponseProto.Status.NOT_LEADER =>
      RegisterClientNotLeader(Option(proto.hasLeaderHint).collect { case true => proto.getLeaderHint }.map(addressProto => Address(addressProto)))
    case RegisterClientResponseProto.Status.OK => RegisterClientSuccessful(proto.getClientId)
    case RegisterClientResponseProto.Status.UNRECOGNIZED => throw new Exception("Register client response returned status Unrecognized")
  }
}
