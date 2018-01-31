package rx.distributed.raft.consensus.state


import rx.distributed.raft.consensus.config.ClusterConfig
import rx.distributed.raft.consensus.server.Address.ServerAddress
import rx.distributed.raft.log.RaftLog

/** Volatile State on leaders - reinitialize after election
  *
  * @param state serverAddress -> (nextIndex, matchIndex)
  */
case class LeaderState private(state: Map[ServerAddress, (Int, Int)]) {
  def nextIndex(server: ServerAddress): Int = state(server)._1 // for each server, index of next log entry to send to that server (init to leader last log index + 1)

  def matchIndex(server: ServerAddress): Int = state(server)._2 // for each server, index of highest log entry known to be replicated on server (init to 0, increase monotonically)

  def nextIndexes: Map[ServerAddress, Int] = state.map(pair => (pair._1, pair._2._1))

  def matchIndexes: Map[ServerAddress, Int] = state.map(pair => (pair._1, pair._2._2))

  def highestReplicatedIndex(config: ClusterConfig): Int = {
    val votingMatchIndexes: Iterable[Int] = config.votingServers.toList.map(server => matchIndex(server.address))
    val indexFrequencies = votingMatchIndexes.foldLeft(Map[Int, Int]())((m, index) => m + (index -> (m.getOrElse(index, 0) + 1))).toSeq.sortBy(_._1)
    val numServers = votingMatchIndexes.size
    indexFrequencies.tail.foldLeft(indexFrequencies.head) { case ((replIndex, serversMissing), (index, count)) =>
      if (numServers - serversMissing > numServers / 2) (index, serversMissing + count)
      else (replIndex, serversMissing + count)
    }._1
  }

  def removeServer(server: ServerAddress): LeaderState = {
    new LeaderState(state - server)
  }

  def addServer(server: ServerAddress, log: RaftLog): LeaderState = {
    if (!state.contains(server))
      new LeaderState(state + (server -> (log.lastIndex + 1, 0)))
    else this
  }

  // update nextIndex and matchIndex for server
  def update(server: ServerAddress, nIndex: Int, mIndex: Int): LeaderState = {
    if (state.contains(server)) new LeaderState(state + (server -> (nIndex, mIndex max matchIndex(server))))
    else this
  }

  // decrement nextIndex for server
  def decrementNextIndex(server: ServerAddress): LeaderState = {
    val nIndex: Int = (nextIndex(server) - 1) max 1
    new LeaderState(state + (server -> (nIndex, matchIndex(server))))
  }
}

object LeaderState {
  def apply(log: RaftLog): LeaderState = {
    val state = log.clusterConfig.cluster.foldLeft(Map[ServerAddress, (Int, Int)]())((map, s) => map + (s.address -> (log.lastIndex + 1, 0)))
    LeaderState(state)
  }

  def apply(log: RaftLog, clusterConfig: ClusterConfig): LeaderState = {
    val state = clusterConfig.cluster.foldLeft(Map[ServerAddress, (Int, Int)]())((map, s) => map + (s.address -> (log.lastIndex + 1, 0)))
    LeaderState(state)
  }
}

