package rx.distributed.raft.consensus.transitions

import org.junit.Test
import org.scalatest.MustMatchers._
import rx.distributed.raft.Utils
import rx.distributed.raft.Utils._
import rx.distributed.raft.consensus.config.ClusterConfig
import rx.distributed.raft.consensus.server.Address.TestingAddress
import rx.distributed.raft.consensus.server.ServerInfo.VotingServer
import rx.distributed.raft.consensus.state.PersistentState
import rx.distributed.raft.consensus.state.RaftState.{Follower, Leader}
import rx.distributed.raft.events.client.{ClientCommandEvent, KeepAliveEvent, RegisterClientEvent}
import rx.distributed.raft.events.consensus._
import rx.distributed.raft.events.internal._
import rx.distributed.raft.events.rpc.response.{AppendEntriesFailure, AppendEntriesSuccess}
import rx.distributed.raft.log._
import rx.lang.scala.Observer

import scala.collection.immutable.HashSet

class RaftLeaderTest {
  private val ADDRESS = TestingAddress("test_server_1")
  private val SERVER2 = TestingAddress("test_server_2")
  private val SERVER3 = TestingAddress("test_server_3")
  private val CANDIDATE_ADDRESS = SERVER3
  private val OTHER_LEADER_ADDRESS = SERVER2
  private val consensusState = Utils.createLeader(1, ADDRESS)
  private val leaderState = createFollower(0, ADDRESS).becomeCandidate().appendNoOp().becomeLeader().appendNoOp().appendNoOp()

  /**
    * Election timeout
    */

  @Test
  def electionTimeout(): Unit = {
    val (newState, event) = StateTransitions.applyEvent(consensusState, ElectionTimeout())
    event mustBe Set(StopHeartbeatTimeout())
    newState.currentTerm mustBe consensusState.currentTerm
    newState.log mustBe consensusState.log
    newState.votedFor mustBe Some(ADDRESS)
    newState.leaderHint mustBe None
    newState.volatileState mustBe consensusState.volatileState
    newState.state mustBe an[Follower]
  }

  /**
    * Heartbeat timeout
    */

  @Test
  def heartbeatTimeout(): Unit = {
    val (newState, events) = StateTransitions.applyEvent(leaderState, HeartbeatTimeout())
    val appendEntriesEvents = events.collect { case appendEntries: AppendEntriesToServer => appendEntries }
    appendEntriesEvents.size mustBe leaderState.log.clusterConfig.size - 1
    appendEntriesEvents.map(_.server) mustBe leaderState.log.clusterConfig.otherClusterServers(ADDRESS)
    appendEntriesEvents.map(_.appendEntries).foreach(appendEntries => {
      appendEntries.prevLogTerm mustBe 1
      appendEntries.prevLogIndex mustBe 1
      appendEntries.entries.size mustBe 2
      appendEntries.entries.collect { case entry: LeaderNoOpEntry => entry }.size mustBe 2
    })
    newState mustBe leaderState
  }

  /**
    * Request vote
    */

  @Test
  def requestVoteSameTerm(): Unit = {
    val voteRequest = VoteRequest(1, CANDIDATE_ADDRESS, 0, 0)
    val (newState, event) = StateTransitions.applyEvent(consensusState, voteRequest)
    event mustBe Set(Reject())
    newState mustBe consensusState
  }

  @Test
  def requestVoteLowerTerm(): Unit = {
    val voteRequest = VoteRequest(0, CANDIDATE_ADDRESS, 0, 0)
    val (newState, event) = StateTransitions.applyEvent(consensusState, voteRequest)
    event mustBe Set(Reject())
    newState mustBe consensusState
  }

  @Test
  def requestVoteHigherTerm(): Unit = {
    val voteRequest = VoteRequest(2, CANDIDATE_ADDRESS, 0, 0)
    val (newState, event) = StateTransitions.applyEvent(consensusState, voteRequest)
    event mustBe Set(SyncToDiskAndThen(newState.persistentState, Accept()), StopHeartbeatTimeout(), ResetElectionTimeout())
    newState.currentTerm mustBe 2
    newState.log mustBe consensusState.log
    newState.votedFor mustBe Some(voteRequest.address)
    newState.leaderHint mustBe None
    newState.volatileState mustBe consensusState.volatileState
    newState.state mustBe Follower()
  }

  /**
    * Append entries
    */

  @Test
  def appendEntriesLowerTerm(): Unit = {
    val appendEntries = AppendEntries(0, OTHER_LEADER_ADDRESS, 0, 0, Seq(clientLogEntry(0)), 0)
    val (newState, event) = StateTransitions.applyEvent(consensusState, appendEntries)
    event mustBe Set(Reject())
    newState mustBe consensusState
  }

  @Test
  def appendEntriesSameTerm(): Unit = {
    val appendEntries = AppendEntries(1, OTHER_LEADER_ADDRESS, 0, 0, Seq(clientLogEntry(1)), 0)
    val (newState, event) = StateTransitions.applyEvent(consensusState, appendEntries)
    event mustBe Set(Reject())
    newState mustBe consensusState
  }

  @Test
  def appendEntriesHigherTerm(): Unit = {
    val appendEntries = AppendEntries(2, OTHER_LEADER_ADDRESS, 0, 0, Seq(clientLogEntry(2)), 0)
    val (newState, event) = StateTransitions.applyEvent(consensusState, appendEntries)
    event mustBe Set(SyncToDiskAndThen(newState.persistentState, Accept()), StopHeartbeatTimeout(), ResetElectionTimeout())
    newState.currentTerm mustBe appendEntries.term
    newState.log.size mustBe 1
    newState.log.getEntriesBetween(0, 1) mustBe appendEntries.entries
    newState.votedFor mustBe None
    newState.leaderHint mustBe Some(OTHER_LEADER_ADDRESS)
    newState.state mustBe Follower()

  }

  /**
    * Append entries failure
    */

  @Test
  def appendEntriesFailureLowerTerm(): Unit = {
    val appendEntriesFail = AppendEntriesFailure(0, SERVER2)
    val (newState, event) = StateTransitions.applyEvent(consensusState, appendEntriesFail)
    event mustBe Set()
    newState mustBe consensusState
  }

  @Test
  def appendEntriesFailureSameTerm(): Unit = {
    val appendEntriesFail = AppendEntriesFailure(1, SERVER2)
    leaderState.state.asInstanceOf[Leader].leaderState.nextIndex(SERVER2) mustBe 2
    leaderState.state.asInstanceOf[Leader].leaderState.nextIndex(SERVER3) mustBe 2
    val (newState, events) = StateTransitions.applyEvent(leaderState, appendEntriesFail)
    val appendEntriesEvents = events.collect { case appendEntries: AppendEntriesToServer => appendEntries }
    appendEntriesEvents.size mustBe 1
    val appendEntriesToServer2 = appendEntriesEvents.head
    appendEntriesToServer2.server mustBe SERVER2
    appendEntriesToServer2.appendEntries.prevLogTerm mustBe -1
    appendEntriesToServer2.appendEntries.prevLogIndex mustBe 0
    appendEntriesToServer2.appendEntries.entries.size mustBe 3
    appendEntriesToServer2.appendEntries.entries.count(_.isInstanceOf[LeaderNoOpEntry]) mustBe 3
    newState.currentTerm mustBe leaderState.currentTerm
    newState.log mustBe leaderState.log
    newState.votedFor mustBe leaderState.votedFor
    newState.leaderHint mustBe leaderState.leaderHint
    newState.volatileState mustBe leaderState.volatileState
    newState.state mustBe an[Leader]
    val newLeaderState = newState.state.asInstanceOf[Leader].leaderState
    newLeaderState.nextIndex(SERVER2) mustBe 1
    newLeaderState.nextIndex(SERVER3) mustBe 2
  }

  @Test
  def appendEntriesFailureHigherTerm(): Unit = {
    val appendEntriesFail = AppendEntriesFailure(2, SERVER2)
    val (newState, event) = StateTransitions.applyEvent(leaderState, appendEntriesFail)
    event mustBe Set(StopHeartbeatTimeout(), ResetElectionTimeout())
    newState.currentTerm mustBe appendEntriesFail.term
    newState.log mustBe leaderState.log
    newState.votedFor mustBe None
    newState.leaderHint mustBe None
    newState.volatileState mustBe leaderState.volatileState
    newState.state mustBe Follower()
  }

  /**
    * Append entries success
    */

  @Test
  def appendEntriesSuccessSingleServerCluster(): Unit = {
    val oneServerConfig = ClusterConfig(HashSet(VotingServer(ADDRESS)))
    val oneServerLeaderState = createFollower(0, ADDRESS, oneServerConfig).becomeCandidate()
      .appendNoOp().becomeLeader().appendNoOp().appendNoOp()
    oneServerLeaderState.volatileState.commitIndex mustBe 0
    val appendEntriesSuccess = AppendEntriesSuccess(1, ADDRESS, 1)
    val (newState, event) = StateTransitions.applyEvent(oneServerLeaderState, appendEntriesSuccess)
    event mustBe Set(CommitEntries(0, 1))
    newState.state mustBe an[Leader]
    newState.currentTerm mustBe oneServerLeaderState.currentTerm
    newState.log mustBe oneServerLeaderState.log
    newState.votedFor mustBe oneServerLeaderState.votedFor
    newState.leaderHint mustBe oneServerLeaderState.leaderHint
    newState.volatileState.commitIndex mustBe 1
    newState.state mustBe an[Leader]
    val newLeaderState = newState.state.asInstanceOf[Leader].leaderState
    newLeaderState.matchIndex(ADDRESS) mustBe 1
    newLeaderState.nextIndex(ADDRESS) mustBe 4
  }

  @Test
  def appendEntriesSuccess2ServerCluster(): Unit = {
    val twoServerConfig = ClusterConfig(HashSet(VotingServer(ADDRESS), VotingServer(SERVER2)))
    val twoServerLeaderState = createFollower(0, ADDRESS, twoServerConfig).becomeCandidate()
      .appendNoOp().becomeLeader().appendNoOp().appendNoOp()
    val appendEntriesSuccess = AppendEntriesSuccess(1, ADDRESS, 1)
    val (newState, event) = StateTransitions.applyEvent(twoServerLeaderState, appendEntriesSuccess)
    event mustBe Set()
    newState.state mustBe an[Leader]
    newState.currentTerm mustBe twoServerLeaderState.currentTerm
    newState.log mustBe twoServerLeaderState.log
    newState.votedFor mustBe twoServerLeaderState.votedFor
    newState.leaderHint mustBe twoServerLeaderState.leaderHint
    newState.volatileState mustBe twoServerLeaderState.volatileState
    newState.state mustBe an[Leader]
    val newLeaderState = newState.state.asInstanceOf[Leader].leaderState
    newLeaderState.matchIndex(ADDRESS) mustBe 1
    newLeaderState.nextIndex(ADDRESS) mustBe 4
    val appendEntriesSuccess2 = AppendEntriesSuccess(1, SERVER2, 2)
    val (newState2, event2) = StateTransitions.applyEvent(newState, appendEntriesSuccess2)
    event2 mustBe Set(CommitEntries(0, 1))
    newState2.state mustBe an[Leader]
    newState2.currentTerm mustBe twoServerLeaderState.currentTerm
    newState2.log mustBe twoServerLeaderState.log
    newState2.votedFor mustBe twoServerLeaderState.votedFor
    newState2.leaderHint mustBe twoServerLeaderState.leaderHint
    newState2.volatileState.commitIndex mustBe 1
    newState2.state mustBe an[Leader]
    val newLeaderState2 = newState2.state.asInstanceOf[Leader].leaderState
    newLeaderState2.matchIndex(SERVER2) mustBe 2
    newLeaderState2.nextIndex(ADDRESS) mustBe 4
  }

  @Test
  def appendEntriesSuccess3ServerCluster(): Unit = {
    val appendEntriesSuccess = AppendEntriesSuccess(1, ADDRESS, 1)
    val (newState, event) = StateTransitions.applyEvent(leaderState, appendEntriesSuccess)
    event mustBe Set()
    newState.state mustBe an[Leader]
    newState.currentTerm mustBe leaderState.currentTerm
    newState.log mustBe leaderState.log
    newState.votedFor mustBe leaderState.votedFor
    newState.leaderHint mustBe leaderState.leaderHint
    newState.volatileState mustBe leaderState.volatileState
    newState.state mustBe an[Leader]
    val newLeaderState = newState.state.asInstanceOf[Leader].leaderState
    newLeaderState.matchIndex(ADDRESS) mustBe 1
    newLeaderState.nextIndex(ADDRESS) mustBe 4
    val appendEntriesSuccess2 = AppendEntriesSuccess(1, SERVER2, 2)
    val (newState2, event2) = StateTransitions.applyEvent(newState, appendEntriesSuccess2)
    event2 mustBe Set(CommitEntries(0, 1))
    newState2.state mustBe an[Leader]
    newState2.currentTerm mustBe leaderState.currentTerm
    newState2.log mustBe leaderState.log
    newState2.votedFor mustBe leaderState.votedFor
    newState2.leaderHint mustBe leaderState.leaderHint
    newState2.volatileState.commitIndex mustBe 1
    newState2.state mustBe an[Leader]
    val newLeaderState2 = newState2.state.asInstanceOf[Leader].leaderState
    newLeaderState2.matchIndex(SERVER2) mustBe 2
    newLeaderState2.nextIndex(ADDRESS) mustBe 4
  }

  /**
    * Log events
    */

  @Test
  def registerClientCommand(): Unit = {
    val registerClient = RegisterClientEvent(Seq(), TIMESTAMP)(Observer())
    val (newState, events) = StateTransitions.applyEvent(leaderState, registerClient)
    assertLeaderOutputEvents(events, newState.persistentState, registerClient.toLogEntry(leaderState.persistentState))
    newState.currentTerm mustBe leaderState.currentTerm
    newState.log.size mustBe leaderState.log.size + 1
    newState.votedFor mustBe leaderState.votedFor
    newState.leaderHint mustBe leaderState.leaderHint
    newState.volatileState mustBe leaderState.volatileState
    newState.log.getEntriesAfter(newState.log.lastIndex - 1).size mustBe 1
    newState.log.getEntriesAfter(newState.log.lastIndex - 1).head mustBe an[RegisterClientEntry]
    val registerClientLogEntry = newState.log.getEntriesAfter(newState.log.lastIndex - 1).head.asInstanceOf[RegisterClientEntry]
    registerClientLogEntry.term mustBe newState.currentTerm
    registerClientLogEntry.clientId mustBe newState.log.lastIndex.toString
  }

  @Test
  def clientCommand(): Unit = {
    val clientCommandEvent = ClientCommandEvent("client", 0, 0, TIMESTAMP, emptyClientCommand())(Observer())
    val (newState, events) = StateTransitions.applyEvent(leaderState, clientCommandEvent)
    assertLeaderOutputEvents(events, newState.persistentState, clientCommandEvent.toLogEntry(leaderState.persistentState))
    newState.currentTerm mustBe leaderState.currentTerm
    newState.log.size mustBe leaderState.log.size + 1
    newState.votedFor mustBe leaderState.votedFor
    newState.leaderHint mustBe leaderState.leaderHint
    newState.volatileState mustBe leaderState.volatileState
    newState.log.getEntriesAfter(newState.log.lastIndex - 1).size mustBe 1
    newState.log.getEntriesAfter(newState.log.lastIndex - 1).head mustBe an[ClientLogEntry]
    val clientLogEntry = newState.log.getEntriesAfter(newState.log.lastIndex - 1).head.asInstanceOf[ClientLogEntry]
    clientLogEntry.term mustBe newState.currentTerm
    clientLogEntry.event mustBe clientCommandEvent
  }

  @Test
  def keepAlive(): Unit = {
    val keepAliveEvent = KeepAliveEvent("client", 0, TIMESTAMP)(Observer())
    val (newState, events) = StateTransitions.applyEvent(leaderState, keepAliveEvent)
    assertLeaderOutputEvents(events, newState.persistentState, keepAliveEvent.toLogEntry(leaderState.persistentState))
    newState.currentTerm mustBe leaderState.currentTerm
    newState.log.size mustBe leaderState.log.size + 1
    newState.votedFor mustBe leaderState.votedFor
    newState.leaderHint mustBe leaderState.leaderHint
    newState.volatileState mustBe leaderState.volatileState
    newState.log.getEntriesAfter(newState.log.lastIndex - 1).size mustBe 1
    newState.log.getEntriesAfter(newState.log.lastIndex - 1).head mustBe an[KeepAliveEntry]
    val keepAliveLogEntry = newState.log.getEntriesAfter(newState.log.lastIndex - 1).head.asInstanceOf[KeepAliveEntry]
    keepAliveLogEntry.term mustBe newState.currentTerm
    keepAliveLogEntry.event mustBe keepAliveEvent
  }

  def assertLeaderOutputEvents(events: Set[InternalEvent], nextPersistentState: PersistentState, logEntry: LogEntry): Unit = {
    val selfAppendEntriesSuccess = SelfAppendEntriesSuccess(AppendEntriesSuccess(nextPersistentState.currentTerm, ADDRESS, nextPersistentState.log.lastIndex))
    events.contains(SyncToDiskAndThen(nextPersistentState, selfAppendEntriesSuccess)) mustBe true
    val appendEntriesEvents = events.collect { case appendEntries: AppendEntriesToServer => appendEntries }
    appendEntriesEvents.size mustBe leaderState.log.clusterConfig.size - 1
    appendEntriesEvents.map(_.server) mustBe leaderState.log.clusterConfig.otherClusterServers(ADDRESS)
    appendEntriesEvents.map(_.appendEntries).foreach(appendEntries => {
      appendEntries.prevLogTerm mustBe 1
      appendEntries.prevLogIndex mustBe 1
      appendEntries.entries.size mustBe 3
      appendEntries.entries.collect { case entry: LeaderNoOpEntry => entry }.size mustBe 2
      appendEntries.entries.contains(logEntry) mustBe true
    })
  }
}
