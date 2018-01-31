package rx.distributed.raft.consensus.config

import rx.distributed.raft.consensus.server.Address.ServerAddress
import rx.distributed.raft.consensus.server.ServerInfo.{ServerType, VotingServer}

case class ClusterConfig(cluster: Set[ServerType]) {

  def canVote(address: ServerAddress): Boolean =
    getServer(address).map(votingServers.contains(_)) match {
      case Some(isContained) => isContained
      case None => false
    }

  def otherClusterServers(address: ServerAddress): Set[ServerAddress] = clusterServers.filter(_ != address)

  def clusterServers: Set[ServerAddress] = cluster.map(_.address)

  def votingServers: Set[ServerType] = cluster.collect { case s: VotingServer => s }

  def getServer(address: ServerAddress): Option[ServerType] = cluster.find(_.address == address)

  def size: Int = cluster.size

  def addServer(server: ServerType): ClusterConfig = ClusterConfig(cluster + server)

  def removeServer(address: ServerAddress): ClusterConfig = {
    getServer(address) match {
      case Some(server) => ClusterConfig(cluster - server)
      case None => this
    }
  }

  def contains(address: ServerAddress): Boolean = cluster.map(_.address).contains(address)
}
