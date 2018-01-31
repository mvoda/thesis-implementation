package rx.distributed.raft.consensus.state

import org.apache.logging.log4j.scala.Logging
import rx.distributed.raft.consensus.PersistentStateIO
import rx.distributed.raft.consensus.config.Config
import rx.distributed.raft.consensus.server.Address.ServerAddress
import rx.distributed.raft.consensus.state.RaftState.{Candidate, Follower, Leader, RaftState}
import rx.distributed.raft.events.consensus.{AppendEntries, VoteRequest}
import rx.distributed.raft.events.rpc.response.VoteGranted
import rx.distributed.raft.log.{LogEntry, RaftLog}

case class ConsensusState(persistentState: PersistentState, volatileState: VolatileState, config: Config,
                          address: ServerAddress, state: RaftState) extends Logging {
  def currentTerm: Int = persistentState.currentTerm

  def votedFor: Option[ServerAddress] = persistentState.votedFor

  def log: RaftLog = persistentState.log

  def commitIndex: Int = volatileState.commitIndex

  def leaderHint: Option[ServerAddress] = persistentState.leaderHint

  def increaseTerm(): ConsensusState = ConsensusState(persistentState.increaseTerm(), volatileState, config, address, state)

  def becomeCandidate(): ConsensusState = updatePersistentState(persistentState.increaseTerm().voteFor(address)).toCandidate

  def becomeFollower(): ConsensusState = toFollower

  def becomeLeader(): ConsensusState = toLeader

  def handleHigherTerm(requestTerm: Int): ConsensusState = requestTerm match {
    case higherTerm if higherTerm > currentTerm => updatePersistentState(persistentState.updateTerm(higherTerm)).toFollower
    case _ =>
      logger.warn("called handle higher term, but request term was lower or equal")
      this
  }

  def updateLeaderHint(address: ServerAddress): ConsensusState = updatePersistentState(persistentState.updateLeaderHint(address))

  def voteFor(address: ServerAddress): ConsensusState = updatePersistentState(persistentState.voteFor(address))

  def isNotWithinMinimumTimeout(voteRequest: VoteRequest): Boolean =
    voteRequest.receivedTimestamp - volatileState.lastLeaderContact > config.timeoutMin

  def canVote(voteRequest: VoteRequest): Boolean = {
    isNotWithinMinimumTimeout(voteRequest) && (votedFor.isEmpty || votedFor.contains(voteRequest.address)) &&
      !log.isMoreUpToDate(voteRequest.lastLogIndex, voteRequest.lastLogTerm)
  }

  def addEntry(entry: LogEntry, prevLogIndex: Int): ConsensusState = updatePersistentState(persistentState.appendEntry(entry, prevLogIndex))

  //TODO: make sure configs update here when re-implementing cluster membership changes
  def appendEntries(appendEntries: AppendEntries): ConsensusState =
    updatePersistentState(persistentState.appendEntries(appendEntries.entries, appendEntries.prevLogIndex).updateLeaderHint(appendEntries.address))
      .updateCommitIndex(appendEntries.leaderCommit)

  def appendNoOp(): ConsensusState = updatePersistentState(persistentState.appendNoOp())

  def appendEntry(entry: LogEntry): ConsensusState = updatePersistentState(persistentState.appendEntry(entry))

  def canAppendEntries(appendEntries: AppendEntries): Boolean =
    log.isConsistent(appendEntries.prevLogIndex, appendEntries.prevLogTerm)

  def validVoter(voterAddress: ServerAddress): Boolean = log.clusterConfig.canVote(voterAddress)

  def votedByMajority(votes: Set[VoteGranted]): Boolean = votes.size > log.clusterConfig.votingServers.size / 2

  def addVote(vote: VoteGranted): ConsensusState = state match {
    case Candidate(votes) => updateRaftState(Candidate(votes + vote))
    case _ => this
  }

  def updateCommitIndex(leaderCommitIndex: Int): ConsensusState =
    if (leaderCommitIndex > commitIndex) updateVolatileState(volatileState.updateCommitIndex(Math.min(leaderCommitIndex, log.lastIndex)))
    else this

  def updateLeaderCommitIndex(): ConsensusState = state match {
    case Leader(leaderState) if leaderState.highestReplicatedIndex(log.clusterConfig) > commitIndex
      && log.getTerm(leaderState.highestReplicatedIndex(log.clusterConfig)) == currentTerm
    => updateVolatileState(volatileState.updateCommitIndex(leaderState.highestReplicatedIndex(log.clusterConfig)))
    case _ => this
  }

  def updateLastLeaderContact(timestamp: Long): ConsensusState =
    if (timestamp > volatileState.lastLeaderContact) updateVolatileState(volatileState.updateLastLeaderContact(timestamp))
    else this

  def decrementNextIndex(address: ServerAddress): ConsensusState = state match {
    case Leader(leaderState) => updateRaftState(Leader(leaderState.decrementNextIndex(address)))
    case _ => this
  }

  def successfullyReplicatedEntries(address: ServerAddress, matchIndex: Int): ConsensusState = state match {
    case Leader(leaderState) => updateRaftState(Leader(leaderState.update(address, log.lastIndex + 1, matchIndex))).updateLeaderCommitIndex()
    case _ => this
  }

  def validServer(address: ServerAddress): Boolean = log.clusterConfig.contains(address)

  private def updatePersistentState(newPersistentState: PersistentState): ConsensusState =
    ConsensusState(newPersistentState, volatileState, config, address, state)

  private def updateRaftState(newState: RaftState): ConsensusState =
    ConsensusState(persistentState, volatileState, config, address, newState)

  private def updateVolatileState(newVolatileState: VolatileState): ConsensusState =
    ConsensusState(persistentState, newVolatileState, config, address, state)

  // TODO: add here custom rules that do not allow non-voting members to transition to followers too soon?

  private def toCandidate: ConsensusState = updateRaftState(Candidate())

  private def toFollower: ConsensusState = updateRaftState(Follower())

  private def toLeader: ConsensusState = updateRaftState(Leader(LeaderState(log)))

}

object ConsensusState {
  def apply(persistentStateIO: PersistentStateIO, config: Config, bootstrapLog: RaftLog): ConsensusState = {
    val persistentState = PersistentState.apply(persistentStateIO, bootstrapLog)
    ConsensusState(persistentState, VolatileState(), config, persistentStateIO.address, Follower())
  }
}
