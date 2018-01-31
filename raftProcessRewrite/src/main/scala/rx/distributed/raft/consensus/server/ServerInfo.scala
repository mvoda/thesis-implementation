package rx.distributed.raft.consensus.server

import rx.distributed.raft.consensus.server.Address.ServerAddress

object ServerInfo {

  sealed trait ServerType {
    def address: ServerAddress

    override def hashCode: Int = address.toString.hashCode

    override def equals(other: Any): Boolean = {
      other match {
        case otherServer: ServerType if this.hashCode == other.hashCode() => address == otherServer.address
        case _ => false
      }
    }

    override def toString: String = address.toString
  }

  case class NonVotingServer(address: ServerAddress, round: Int = 1) extends ServerType {
    val roundStart: Long = System.currentTimeMillis()
  }

  case class VotingServer(address: ServerAddress) extends ServerType

}
