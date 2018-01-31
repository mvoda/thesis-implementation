package rx.distributed.raft.consensus.transitions

import org.junit.Test
import org.scalatest.MustMatchers._
import rx.distributed.raft.Utils
import rx.distributed.raft.Utils._
import rx.distributed.raft.consensus.config.ClusterConfig
import rx.distributed.raft.consensus.server.Address.TestingAddress
import rx.distributed.raft.consensus.server.ServerInfo.{NonVotingServer, VotingServer}
import rx.distributed.raft.consensus.state.RaftState.{Candidate, Follower, Leader}
import rx.distributed.raft.consensus.state.VolatileState
import rx.distributed.raft.events.client.{ClientCommandEvent, KeepAliveEvent, RegisterClientEvent}
import rx.distributed.raft.events.consensus._
import rx.distributed.raft.events.internal._
import rx.distributed.raft.events.rpc.response.{AppendEntriesSuccess, VoteGranted, VoteRejected}
import rx.lang.scala.Observer

import scala.collection.immutable.HashSet

class RaftCandidateTest {
  private val ADDRESS = TestingAddress("test_server_1")
  private val SERVER2 = TestingAddress("test_server_2")
  private val CANDIDATE_ADDRESS = SERVER2
  private val LEADER_ADDRESS = TestingAddress("test_server_3")
  private val consensusState = Utils.createCandidate(1, ADDRESS)

  /**
    * Election timeout
    */

  @Test
  def electionTimeoutTest(): Unit = {
    val (newState, events) = StateTransitions.applyEvent(consensusState, ElectionTimeout())
    events.size mustBe consensusState.log.clusterConfig.size
    events.count(_.isInstanceOf[RequestVotesFromServer]) mustBe consensusState.log.clusterConfig.size - 1
    events.count(_.isInstanceOf[SyncToDiskAndThen]) mustBe 1
    events.collect { case e: SyncToDiskAndThen => e }.head.nextEvent mustBe an[SelfVoteGranted]
    val voteRequests = events.collect { case voteReq: RequestVotesFromServer => voteReq }
    voteRequests.map(_.server) mustBe consensusState.log.clusterConfig.otherClusterServers(ADDRESS)
    voteRequests.head.voteRequest.address mustBe ADDRESS
    voteRequests.head.voteRequest.term mustBe newState.currentTerm
    voteRequests.head.voteRequest.lastLogIndex mustBe newState.log.lastIndex
    voteRequests.head.voteRequest.lastLogTerm mustBe newState.log.lastTerm
    newState.currentTerm mustBe consensusState.currentTerm + 1
    newState.state mustBe Candidate(Set())
    newState.votedFor mustBe Some(ADDRESS)
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
    event mustBe Set(SyncToDiskAndThen(newState.persistentState, Accept()), ResetElectionTimeout())
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
  def appendEntriesSameTerm(): Unit = {
    val appendEntries = AppendEntries(1, LEADER_ADDRESS, 0, 0, Seq(clientLogEntry(1)), 0, TIMESTAMP)
    val (newState, event) = StateTransitions.applyEvent(consensusState, appendEntries)
    event mustBe Set(SyncToDiskAndThen(newState.persistentState, Accept()), ResetElectionTimeout())
    newState.currentTerm mustBe appendEntries.term
    newState.log.size mustBe 1
    newState.log.getEntriesBetween(0, 1) mustBe appendEntries.entries
    newState.votedFor mustBe consensusState.votedFor
    newState.leaderHint mustBe Some(LEADER_ADDRESS)
    newState.volatileState mustBe VolatileState(0, TIMESTAMP)
    newState.state mustBe Follower()
  }

  @Test
  def appendEntriesHigherTerm(): Unit = {
    val appendEntries = AppendEntries(2, LEADER_ADDRESS, 0, 0, Seq(clientLogEntry(2)), 0, TIMESTAMP)
    val (newState, event) = StateTransitions.applyEvent(consensusState, appendEntries)
    event mustBe Set(SyncToDiskAndThen(newState.persistentState, Accept()), ResetElectionTimeout())
    newState.currentTerm mustBe appendEntries.term
    newState.log.size mustBe 1
    newState.log.getEntriesBetween(0, 1) mustBe appendEntries.entries
    newState.votedFor mustBe None
    newState.leaderHint mustBe Some(LEADER_ADDRESS)
    newState.volatileState mustBe VolatileState(0, TIMESTAMP)
    newState.state mustBe Follower()
  }

  @Test
  def appendEntriesLowerTerm(): Unit = {
    val appendEntries = AppendEntries(0, LEADER_ADDRESS, 0, 0, Seq(clientLogEntry(0)), 0)
    val (newState, event) = StateTransitions.applyEvent(consensusState, appendEntries)
    event mustBe Set(Reject())
    newState mustBe consensusState
  }

  /**
    * Votes
    */

  @Test
  def voteRejectedLowerTerm(): Unit = {
    val voteRejected = VoteRejected(0, SERVER2)
    val (newState, event) = StateTransitions.applyEvent(consensusState, voteRejected)
    event mustBe Set()
    newState mustBe consensusState
  }

  @Test
  def voteRejectedSameTerm(): Unit = {
    val voteRejected = VoteRejected(1, SERVER2)
    val (newState, event) = StateTransitions.applyEvent(consensusState, voteRejected)
    event mustBe Set()
    newState mustBe consensusState
  }

  @Test
  def voteRejectedHigherTerm(): Unit = {
    val voteRejected = VoteRejected(2, SERVER2)
    val (newState, event) = StateTransitions.applyEvent(consensusState, voteRejected)
    event mustBe Set(ResetElectionTimeout())
    newState.currentTerm mustBe voteRejected.term
    newState.log mustBe consensusState.log
    newState.votedFor mustBe None
    newState.leaderHint mustBe None
    newState.volatileState mustBe consensusState.volatileState
    newState.state mustBe Follower()
  }

  @Test
  def voteGrantedLowerTerm(): Unit = {
    val voteGranted = VoteGranted(0, SERVER2)
    val (newState, event) = StateTransitions.applyEvent(consensusState, voteGranted)
    event mustBe Set()
    newState mustBe consensusState
  }

  @Test
  def voteGrantedSameTerm(): Unit = {
    val voteGranted = VoteGranted(1, SERVER2)
    val (newState, event) = StateTransitions.applyEvent(consensusState, voteGranted)
    event mustBe Set()
    newState.currentTerm mustBe consensusState.currentTerm
    newState.log mustBe consensusState.log
    newState.votedFor mustBe consensusState.votedFor
    newState.leaderHint mustBe None
    newState.volatileState mustBe consensusState.volatileState
    newState.state mustBe Candidate(Set(voteGranted))
  }

  @Test
  def ignoresVoteFromServerNotInConfig(): Unit = {
    val oneServerConfig = ClusterConfig(HashSet(VotingServer(consensusState.address)))
    val oneServerConsensusState = createCandidate(consensusState.currentTerm, consensusState.address, oneServerConfig)
    val voteGranted = VoteGranted(1, SERVER2)
    val (newState, event) = StateTransitions.applyEvent(oneServerConsensusState, voteGranted)
    event mustBe Set()
    newState mustBe oneServerConsensusState
  }

  @Test
  def ignoresVoteFromNonVotingServerInConfig(): Unit = {
    val twoServerConfig = ClusterConfig(HashSet(VotingServer(consensusState.address), NonVotingServer(SERVER2)))
    val twoServerConsensusState = createCandidate(consensusState.currentTerm, consensusState.address, twoServerConfig)
    val voteGranted = VoteGranted(1, SERVER2)
    val (newState, event) = StateTransitions.applyEvent(twoServerConsensusState, voteGranted)
    event mustBe Set()
    newState mustBe twoServerConsensusState
  }

  @Test
  def leaderTransitionOneServerCluster(): Unit = {
    val oneServerConfig = ClusterConfig(HashSet(VotingServer(consensusState.address)))
    val oneServerConsensusState = createCandidate(consensusState.currentTerm, consensusState.address, oneServerConfig)
    val voteGranted = VoteGranted(1, consensusState.address)
    val (newState, events) = StateTransitions.applyEvent(oneServerConsensusState, voteGranted)
    events.size mustBe 3
    events.contains(ResetHeartbeatTimeout()) mustBe true
    events.contains(ResetElectionTimeout()) mustBe true
    val syncEvents = events.collect { case e: SyncToDiskAndThen => e }
    syncEvents.size mustBe 1
    syncEvents.head.nextEvent mustBe SelfAppendEntriesSuccess(AppendEntriesSuccess(1, ADDRESS, 1))
    newState.currentTerm mustBe oneServerConsensusState.currentTerm
    newState.log.size mustBe oneServerConsensusState.log.size + 1
    newState.votedFor mustBe oneServerConsensusState.votedFor
    newState.leaderHint mustBe None
    newState.volatileState mustBe oneServerConsensusState.volatileState
    newState.state mustBe an[Leader]
  }

  @Test
  def leaderTransition2ServerCluster(): Unit = {
    val twoServerConfig = ClusterConfig(HashSet(VotingServer(consensusState.address), VotingServer(SERVER2)))
    val twoServerConsensusState = createCandidate(consensusState.currentTerm, consensusState.address, twoServerConfig)
    val voteGranted = VoteGranted(1, consensusState.address)
    val (newState, event) = StateTransitions.applyEvent(twoServerConsensusState, voteGranted)
    event mustBe Set()
    newState.log mustBe twoServerConsensusState.log
    newState.state mustBe Candidate(Set(voteGranted))
    val voteGranted2 = VoteGranted(1, SERVER2)
    val (leaderState, event2) = StateTransitions.applyEvent(newState, voteGranted2)
    event2.size mustBe 4
    event2.contains(ResetHeartbeatTimeout()) mustBe true
    event2.contains(ResetElectionTimeout()) mustBe true
    assertLeaderOutputEvents(event2, twoServerConfig)
    leaderState.currentTerm mustBe twoServerConsensusState.currentTerm
    leaderState.log.size mustBe twoServerConsensusState.log.size + 1
    leaderState.votedFor mustBe twoServerConsensusState.votedFor
    leaderState.leaderHint mustBe None
    leaderState.volatileState mustBe twoServerConsensusState.volatileState
    leaderState.state mustBe an[Leader]
  }

  @Test
  def leaderTransition3ServerCluster(): Unit = {
    val voteGranted = VoteGranted(1, consensusState.address)
    val (newState, event) = StateTransitions.applyEvent(consensusState, voteGranted)
    event mustBe Set()
    newState.log mustBe consensusState.log
    newState.state mustBe Candidate(Set(voteGranted))
    val voteGranted2 = VoteGranted(1, SERVER2)
    val (leaderState, event2) = StateTransitions.applyEvent(newState, voteGranted2)
    event2.size mustBe 5
    event2.contains(ResetHeartbeatTimeout()) mustBe true
    event2.contains(ResetElectionTimeout()) mustBe true
    assertLeaderOutputEvents(event2, consensusState.log.clusterConfig)
    leaderState.currentTerm mustBe consensusState.currentTerm
    leaderState.log.size mustBe consensusState.log.size + 1
    leaderState.votedFor mustBe consensusState.votedFor
    leaderState.leaderHint mustBe None
    leaderState.volatileState mustBe consensusState.volatileState
    leaderState.state mustBe an[Leader]
  }

  @Test
  def doesNotCountDuplicateVotes(): Unit = {
    val voteGranted = VoteGranted(1, SERVER2)
    val (newState, event) = StateTransitions.applyEvent(consensusState, voteGranted)
    event mustBe Set()
    newState.state mustBe Candidate(Set(voteGranted))
    val (newState2, event2) = StateTransitions.applyEvent(newState, voteGranted)
    event2 mustBe Set()
    newState2.currentTerm mustBe consensusState.currentTerm
    newState2.log mustBe consensusState.log
    newState2.votedFor mustBe consensusState.votedFor
    newState2.leaderHint mustBe None
    newState2.volatileState mustBe consensusState.volatileState
    newState2.state mustBe Candidate(Set(voteGranted))
  }

  /**
    * Log events
    */

  @Test
  def registerClientCommand(): Unit = {
    val registerClient = RegisterClientEvent(Seq(), TIMESTAMP)(Observer())
    val (newState, event) = StateTransitions.applyEvent(consensusState, registerClient)
    event mustBe Set(NotLeader())
    newState mustBe consensusState
  }

  @Test
  def clientCommand(): Unit = {
    val clientCommandEvent = ClientCommandEvent("client", 0, 0, TIMESTAMP, emptyClientCommand())(Observer())
    val (newState, event) = StateTransitions.applyEvent(consensusState, clientCommandEvent)
    event mustBe Set(NotLeader())
    newState mustBe consensusState
  }

  @Test
  def keepAlive(): Unit = {
    val keepAliveEvent = KeepAliveEvent("client", 0, TIMESTAMP)(Observer())
    val (newState, event) = StateTransitions.applyEvent(consensusState, keepAliveEvent)
    event mustBe Set(NotLeader())
    newState mustBe consensusState
  }

  def assertLeaderOutputEvents(events: Set[InternalEvent], clusterConfig: ClusterConfig): Unit = {
    val appendEntriesEvents = events.collect { case appendEntries: AppendEntriesToServer => appendEntries }
    appendEntriesEvents.size mustBe clusterConfig.size - 1
    appendEntriesEvents.map(_.server) mustBe clusterConfig.otherClusterServers(ADDRESS)
    appendEntriesEvents.map(_.appendEntries).foreach(appendEntries => {
      appendEntries.prevLogTerm mustBe 1
      appendEntries.prevLogIndex mustBe 1
      appendEntries.entries.size mustBe 0
    })
  }
}
