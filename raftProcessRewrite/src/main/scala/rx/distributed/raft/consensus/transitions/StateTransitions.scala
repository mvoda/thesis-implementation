package rx.distributed.raft.consensus.transitions

import org.apache.logging.log4j.scala.Logging
import rx.distributed.raft.consensus.server.Address.ServerAddress
import rx.distributed.raft.consensus.state.RaftState.{Candidate, Follower, Leader}
import rx.distributed.raft.consensus.state.{ConsensusState, LeaderState, PersistentState}
import rx.distributed.raft.events.client.ClientEvent
import rx.distributed.raft.events.consensus._
import rx.distributed.raft.events.internal._
import rx.distributed.raft.events.rpc.response.{AppendEntriesFailure, AppendEntriesSuccess, VoteGranted}

object StateTransitions extends Logging {
  def applyEvent(state: ConsensusState, event: ElectionTimeout): (ConsensusState, Set[InternalEvent]) = state.state match {
    case _: Follower | _: Candidate =>
      val nextState = state.becomeCandidate()
      val requestMessage = VoteRequest(nextState.currentTerm, nextState.address, nextState.log.lastIndex, nextState.log.lastTerm)
      val voteRequests: Set[InternalEvent] = nextState.log.clusterConfig.otherClusterServers(state.address)
        .map(server => RequestVotesFromServer(server, requestMessage))
      val selfVoteGranted = SelfVoteGranted(VoteGranted(nextState.currentTerm, nextState.address))
      (nextState, voteRequests + syncToDisk(state.persistentState, nextState.persistentState, selfVoteGranted))
    case Leader(_) => (state.becomeFollower(), Set(StopHeartbeatTimeout()))
    case _ => (state, Set())
  }

  def applyEvent(state: ConsensusState, event: HeartbeatTimeout): (ConsensusState, Set[InternalEvent]) = state.state match {
    case Leader(_) => (state, appendEntriesToCluster(state, state))
    case _ => (state, Set())
  }

  def applyEvent(state: ConsensusState, event: ConsensusEvent): (ConsensusState, Set[InternalEvent]) = event.term match {
    case higherTerm if higherTerm > state.currentTerm => updateTimeouts(state, applyEvent(state.handleHigherTerm(higherTerm), event))
    case lowerTerm if lowerTerm < state.currentTerm => event match {
      case _: ConsensusRequest => (state, Set(Reject()))
      case _: ConsensusResponse => (state, Set())
    }
    case equalTerm if equalTerm == state.currentTerm => event match {
      case request: ConsensusRequest => val (nextState, response, outputEvents) = applyRequest(state, request)
        val nextStateAndEvents = (nextState, outputEvents + syncToDisk(state.persistentState, nextState.persistentState, response))
        updateTimeouts(state, nextStateAndEvents)
      case response: ConsensusResponse => updateTimeouts(state, applyResponse(state, response))
      case _ => (state, Set())
    }
  }

  private def applyRequest(state: ConsensusState, request: ConsensusRequest): (ConsensusState, Response, Set[InternalEvent]) = request match {
    case voteRequest: VoteRequest => applyRequest(state, voteRequest)
    case appendEntries: AppendEntries => applyRequest(state, appendEntries)
  }

  private def applyRequest(state: ConsensusState, request: VoteRequest): (ConsensusState, Response, Set[InternalEvent]) = state.state match {
    case Follower() if state.canVote(request) => (state.voteFor(request.address), Accept(), Set(ResetElectionTimeout()))
    case Follower() if state.isNotWithinMinimumTimeout(request) => (state, Reject(), Set(ResetElectionTimeout()))
    case _ => (state, Reject(), Set())
  }

  private def applyRequest(state: ConsensusState, request: AppendEntries): (ConsensusState, Response, Set[InternalEvent]) = state.state match {
    case Follower() =>
      if (state.canAppendEntries(request)) {
        val nextState = state.appendEntries(request).updateLastLeaderContact(request.receivedTimestamp)
        val commitEntries: Set[InternalEvent] =
          if (nextState.commitIndex > state.commitIndex) Set(CommitEntries(state.commitIndex, nextState.commitIndex))
          else Set()
        (nextState, Accept(), commitEntries + ResetElectionTimeout())
      }
      else (state.updateLastLeaderContact(request.receivedTimestamp), Reject(), Set(ResetElectionTimeout()))
    case Candidate(_) => applyRequest(state.becomeFollower(), request)
    case Leader(_) =>
      logger.warn(s"[${state.address}] Leader received append entries from another leader (${request.address}) on same term. THIS SHOULD NEVER HAPPEN!")
      (state, Reject(), Set())
    case _ => (state, Reject(), Set())
  }

  private def applyResponse(state: ConsensusState, response: ConsensusResponse): (ConsensusState, Set[InternalEvent]) = response match {
    case vote: VoteGranted => applyResponse(state, vote)
    case appendEntriesFailure: AppendEntriesFailure => applyResponse(state, appendEntriesFailure)
    case appendEntriesSuccess: AppendEntriesSuccess => applyResponse(state, appendEntriesSuccess)
    case _ => (state, Set())
  }

  private def applyResponse(state: ConsensusState, vote: VoteGranted): (ConsensusState, Set[InternalEvent]) = state.state match {
    case Candidate(votes) if state.validVoter(vote.address) =>
      if (state.votedByMajority(votes + vote)) {
        val nextState = state.appendNoOp().becomeLeader()
        (nextState, appendEntriesToCluster(state, nextState) + ResetHeartbeatTimeout() + ResetElectionTimeout())
      }
      else (state.addVote(vote), Set())
    case _ => (state, Set())
  }

  private def applyResponse(state: ConsensusState, appendEntriesFailure: AppendEntriesFailure): (ConsensusState, Set[InternalEvent]) =
    state.state match {
      case Leader(_) if state.validServer(appendEntriesFailure.address) =>
        val nextState = state.decrementNextIndex(appendEntriesFailure.address)
        (nextState, Set(appendEntriesToServer(appendEntriesFailure.address, state, nextState)))
      case _ => (state, Set())
    }

  private def applyResponse(state: ConsensusState, appendEntriesSuccess: AppendEntriesSuccess): (ConsensusState, Set[InternalEvent]) =
    state.state match {
      case Leader(_) if state.validServer(appendEntriesSuccess.address) =>
        val nextState = state.successfullyReplicatedEntries(appendEntriesSuccess.address, appendEntriesSuccess.matchIndex)
        if (nextState.commitIndex > state.commitIndex) (nextState, Set(CommitEntries(state.commitIndex, nextState.commitIndex)))
        else (nextState, Set())
      case _ => (state, Set())
    }

  def applyEvent(state: ConsensusState, logCommand: ClientEvent): (ConsensusState, Set[InternalEvent]) = state.state match {
    case Leader(_) =>
      val logEntry = logCommand.toLogEntry(state.persistentState)
      val nextState = state.appendEntry(logEntry)
      (nextState, appendEntriesToCluster(state, nextState))
    case _ => (state, Set(NotLeader()))
  }

  private def appendEntriesToCluster(state: ConsensusState, nextState: ConsensusState): Set[InternalEvent] = {
    val clusterServers = nextState.log.clusterConfig.clusterServers
    clusterServers.map(server => appendEntriesToServer(server, state, nextState))
  }

  private def appendEntriesToServer(address: ServerAddress, state: ConsensusState, nextState: ConsensusState): InternalEvent = {
    if (address == nextState.address) appendEntriesToSelf(state.persistentState, nextState)
    else appendEntriesToFollower(address, nextState)
  }

  private def appendEntriesToSelf(oldPersistentState: PersistentState, nextState: ConsensusState): InternalEvent = {
    val selfAppendEntriesSuccess = SelfAppendEntriesSuccess(AppendEntriesSuccess(nextState.currentTerm, nextState.address, nextState.log.lastIndex))
    syncToDisk(oldPersistentState, nextState.persistentState, selfAppendEntriesSuccess)
  }

  private def appendEntriesToFollower(followerAddress: ServerAddress, state: ConsensusState): InternalEvent = {
    val prevLogIndex = getLeaderState(state).nextIndex(followerAddress) - 1
    val prevLogTerm = state.log.getTerm(prevLogIndex)
    val request = AppendEntries(state.currentTerm, state.address, prevLogIndex, prevLogTerm, state.log.getEntriesAfter(prevLogIndex),
      state.commitIndex)
    AppendEntriesToServer(followerAddress, request)
  }

  private def getLeaderState(state: ConsensusState): LeaderState = state.state.asInstanceOf[Leader].leaderState

  private def updateTimeouts(currentState: ConsensusState, nextStateAndEvents: (ConsensusState, Set[InternalEvent])): (ConsensusState, Set[InternalEvent]) = {
    val updatedStateAndEvents = checkHeartbeatTimeout(currentState, nextStateAndEvents)
    checkElectionTimeout(currentState, updatedStateAndEvents)
  }

  private def checkHeartbeatTimeout(currentState: ConsensusState, nextStateAndEvents: (ConsensusState, Set[InternalEvent])): (ConsensusState, Set[InternalEvent]) = {
    if (nextStateAndEvents._2.contains(StopHeartbeatTimeout())) return nextStateAndEvents
    currentState.state match {
      case Leader(_) =>
        if (!nextStateAndEvents._1.state.isInstanceOf[Leader]) (nextStateAndEvents._1, nextStateAndEvents._2 + StopHeartbeatTimeout())
        else nextStateAndEvents
      case _ => nextStateAndEvents
    }
  }

  private def checkElectionTimeout(currentState: ConsensusState, nextStateAndEvents: (ConsensusState, Set[InternalEvent])): (ConsensusState, Set[InternalEvent]) = {
    if (currentState.currentTerm < nextStateAndEvents._1.currentTerm && !nextStateAndEvents._2.contains(ResetElectionTimeout()))
      (nextStateAndEvents._1, nextStateAndEvents._2 + ResetElectionTimeout())
    else
      nextStateAndEvents
  }

  private def syncToDisk(currentState: PersistentState, nextState: PersistentState, event: InternalEvent): InternalEvent = {
    if (currentState != nextState) SyncToDiskAndThen(nextState, event)
    else event
  }
}
