package rx.distributed.raft.events.rpc.response

import rx.distributed.raft.KeepAliveResponseProto
import rx.distributed.raft.consensus.server.Address
import rx.distributed.raft.consensus.server.Address.ServerAddress

sealed trait KeepAliveResponse extends RpcResponse {
  override def toProtobuf: KeepAliveResponseProto
}

case class KeepAliveSuccessful() extends KeepAliveResponse {
  override def toProtobuf: KeepAliveResponseProto = KeepAliveResponseProto.newBuilder().setStatus(KeepAliveResponseProto.Status.OK).build()
}

case class KeepAliveSessionExpired() extends KeepAliveResponse {
  override def toProtobuf: KeepAliveResponseProto = KeepAliveResponseProto.newBuilder().setStatus(KeepAliveResponseProto.Status.SESSION_EXPIRED).build()
}

case class KeepAliveNotLeader(leaderHint: Option[ServerAddress]) extends KeepAliveResponse with NotLeaderRpcResponse {
  override def toProtobuf: KeepAliveResponseProto = {
    val response = KeepAliveResponseProto.newBuilder().setStatus(KeepAliveResponseProto.Status.NOT_LEADER)
    leaderHint.foreach(address => response.setLeaderHint(address.toProtobuf))
    response.build()
  }
}

object KeepAliveResponse {
  def apply(proto: KeepAliveResponseProto): KeepAliveResponse = proto.getStatus match {
    case KeepAliveResponseProto.Status.OK => KeepAliveSuccessful()
    case KeepAliveResponseProto.Status.SESSION_EXPIRED => KeepAliveSessionExpired()
    case KeepAliveResponseProto.Status.NOT_LEADER =>
      KeepAliveNotLeader(Option(proto.hasLeaderHint).collect { case true => proto.getLeaderHint }.map(addressProto => Address(addressProto)))
  }
}
