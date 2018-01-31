package rx.distributed.raft.consensus.server

import java.io.IOException

import rx.distributed.raft.ServerAddressProto

object Address {

  sealed trait ServerAddress extends Serializable {
    def toProtobuf: ServerAddressProto
  }

  case class RemoteAddress(location: String, port: Int) extends ServerAddress {
    override def toString = s"$location:$port"

    def toProtobuf: ServerAddressProto = ServerAddressProto.newBuilder().setType(ServerAddressProto.ServerType.REMOTE)
      .setLocation(location).setPort(port).build()
  }

  case class TestingAddress(name: String) extends ServerAddress {
    override def toString = s"$name"

    def toProtobuf: ServerAddressProto = ServerAddressProto.newBuilder().setType(ServerAddressProto.ServerType.TESTING)
      .setLocation(toString).build()
  }

  def apply(address: ServerAddressProto): ServerAddress = {
    address.getType match {
      case ServerAddressProto.ServerType.TESTING => TestingAddress(address.getLocation)
      case ServerAddressProto.ServerType.REMOTE => RemoteAddress(address.getLocation, address.getPort)
      case _ => throw new IOException("Unrecognized protobuf server address type: " + address.getType)
    }
  }
}
