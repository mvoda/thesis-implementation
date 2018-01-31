package rx.distributed.raft.consensus.transitions

import org.junit.{After, Before, Test}
import org.scalatest.MustMatchers._
import rx.distributed.raft.Utils
import rx.distributed.raft.Utils._
import rx.distributed.raft.consensus.server.Address.TestingAddress
import rx.distributed.raft.consensus.state.RaftState.{Candidate, Follower}
import rx.distributed.raft.consensus.state.VolatileState
import rx.distributed.raft.events.client.{ClientCommandEvent, KeepAliveEvent, RegisterClientEvent}
import rx.distributed.raft.events.consensus.{AppendEntries, ElectionTimeout, VoteRequest}
import rx.distributed.raft.events.internal._
import rx.distributed.raft.log.NoOpEntry
import rx.lang.scala.Observer

class RaftFollowerTest {
  private val ADDRESS = TestingAddress("test_server_1")
  private val CANDIDATE_ADDRESS = TestingAddress("test_server_2")
  private val LEADER_ADDRESS = TestingAddress("test_server_3")
  private val consensusState = Utils.createFollower(1, ADDRESS)

  @Before
  def beforeEach(): Unit = {

  }

  @After
  def afterEach(): Unit = {

  }

  /**
    * Election timeout
    */

  @Test
  def electionTimeoutTransition(): Unit = {
    val stateWithLeaderHint = consensusState.updateLeaderHint(LEADER_ADDRESS)
    val (newState, events) = StateTransitions.applyEvent(stateWithLeaderHint, ElectionTimeout())
    events.size mustBe stateWithLeaderHint.log.clusterConfig.size
    events.count(_.isInstanceOf[RequestVotesFromServer]) mustBe stateWithLeaderHint.log.clusterConfig.size - 1
    events.count(_.isInstanceOf[SyncToDiskAndThen]) mustBe 1
    events.collect { case e: SyncToDiskAndThen => e }.head.nextEvent mustBe an[SelfVoteGranted]
    val voteRequests = events.collect { case voteReq: RequestVotesFromServer => voteReq }
    voteRequests.map(_.server) mustBe stateWithLeaderHint.log.clusterConfig.otherClusterServers(ADDRESS)
    voteRequests.head.voteRequest.address mustBe ADDRESS
    voteRequests.head.voteRequest.term mustBe newState.currentTerm
    voteRequests.head.voteRequest.lastLogIndex mustBe newState.log.lastIndex
    voteRequests.head.voteRequest.lastLogTerm mustBe newState.log.lastTerm
    newState.currentTerm mustBe stateWithLeaderHint.currentTerm + 1
    newState.log mustBe stateWithLeaderHint.log
    newState.votedFor mustBe Some(ADDRESS)
    newState.leaderHint mustBe None
    newState.volatileState mustBe stateWithLeaderHint.volatileState
    newState.state mustBe Candidate(Set())
  }

  /**
    * Request vote
    */

  @Test
  def requestVoteSameTerm(): Unit = {
    val voteRequest = VoteRequest(1, CANDIDATE_ADDRESS, 0, 0)
    val (newState, event) = StateTransitions.applyEvent(consensusState, voteRequest)
    event mustBe Set(SyncToDiskAndThen(newState.persistentState, Accept()), ResetElectionTimeout())
    newState.currentTerm mustBe consensusState.currentTerm
    newState.log mustBe consensusState.log
    newState.votedFor mustBe Some(voteRequest.address)
    newState.leaderHint mustBe consensusState.leaderHint
    newState.volatileState mustBe consensusState.volatileState
    newState.state mustBe Follower()
  }

  // already voted this term for different server => reject
  @Test
  def requestVoteSameTermAlreadyVoted(): Unit = {
    val voteRequest = VoteRequest(1, CANDIDATE_ADDRESS, 0, 0)
    val stateWithVote = consensusState.voteFor(ADDRESS)
    val (newState, event) = StateTransitions.applyEvent(stateWithVote, voteRequest)
    event mustBe Set(Reject(), ResetElectionTimeout())
    newState mustBe stateWithVote
  }

  @Test
  def requestVoteHigherTerm(): Unit = {
    val voteRequest = VoteRequest(2, CANDIDATE_ADDRESS, 0, 0)
    val (newState, event) = StateTransitions.applyEvent(consensusState, voteRequest)
    event mustBe Set(SyncToDiskAndThen(newState.persistentState, Accept()), ResetElectionTimeout())
    newState.currentTerm mustBe 2
    newState.log mustBe consensusState.log
    newState.votedFor mustBe Some(voteRequest.address)
    newState.leaderHint mustBe consensusState.leaderHint
    newState.volatileState mustBe consensusState.volatileState
    newState.state mustBe Follower()
  }

  @Test
  def requestVoteLowerTerm(): Unit = {
    val voteRequest = VoteRequest(0, CANDIDATE_ADDRESS, 0, 0)
    val (newState, event) = StateTransitions.applyEvent(consensusState, voteRequest)
    event mustBe Set(Reject())
    newState mustBe consensusState
  }


  // already voted this term for same candidate => accept
  @Test
  def requestVoteSameCandidateSameTerm(): Unit = {
    val voteRequest = VoteRequest(consensusState.currentTerm, CANDIDATE_ADDRESS, 0, 0)
    val stateWithVote = consensusState.voteFor(voteRequest.address)
    val (newState, event) = StateTransitions.applyEvent(stateWithVote, voteRequest)
    event mustBe Set(Accept(), ResetElectionTimeout())
    newState mustBe stateWithVote
  }

  // vote request with lagging lastLogIndex
  @Test
  def requestVoteHigherTermLaggingLog(): Unit = {
    val voteRequest = VoteRequest(consensusState.currentTerm, CANDIDATE_ADDRESS, 0, 0)
    val stateWithLogEntry = consensusState.addEntry(NoOpEntry(0, TIMESTAMP), 1)
    val (newState, event) = StateTransitions.applyEvent(stateWithLogEntry, voteRequest)
    event mustBe Set(Reject(), ResetElectionTimeout())
    newState mustBe stateWithLogEntry
  }

  @Test
  def ignoresVoteRequestWithinMinimumElectionTimeout(): Unit = {
    val voteRequest = VoteRequest(consensusState.currentTerm, CANDIDATE_ADDRESS, 0, 0, TIMESTAMP + 10)
    val stateAfterHeartbeat = consensusState.updateLastLeaderContact(TIMESTAMP)
    val (newState, event) = StateTransitions.applyEvent(stateAfterHeartbeat, voteRequest)
    event mustBe Set(Reject())
    newState mustBe stateAfterHeartbeat
  }

  @Test
  def grantsVoteRequestAfterMinimumElectionTimeout(): Unit = {
    val newTimestamp = TIMESTAMP + consensusState.config.timeoutMin + 1
    val voteRequest = VoteRequest(consensusState.currentTerm, CANDIDATE_ADDRESS, 0, 0, newTimestamp)
    val stateAfterHeartbeat = consensusState.updateLastLeaderContact(TIMESTAMP)
    val (newState, event) = StateTransitions.applyEvent(stateAfterHeartbeat, voteRequest)
    event mustBe Set(SyncToDiskAndThen(newState.persistentState, Accept()), ResetElectionTimeout())
    newState.currentTerm mustBe consensusState.currentTerm
    newState.log mustBe consensusState.log
    newState.votedFor mustBe Some(CANDIDATE_ADDRESS)
    newState.leaderHint mustBe consensusState.leaderHint
    newState.volatileState mustBe VolatileState(consensusState.commitIndex, TIMESTAMP)
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
    newState.currentTerm mustBe consensusState.currentTerm
    newState.log.size mustBe 1
    newState.log.getEntriesBetween(0, 1) mustBe appendEntries.entries
    newState.votedFor mustBe None
    newState.leaderHint mustBe Some(LEADER_ADDRESS)
    newState.volatileState mustBe VolatileState(0, TIMESTAMP)
    newState.state mustBe Follower()
  }

  @Test
  def appendMultipleEntriesSameTerm(): Unit = {
    val entries = Seq(clientLogEntry(1), clientLogEntry(1, 1), clientLogEntry(1, 2))
    val appendEntries = AppendEntries(1, LEADER_ADDRESS, 0, 0, entries, 0, TIMESTAMP)
    val (newState, event) = StateTransitions.applyEvent(consensusState, appendEntries)
    event mustBe Set(SyncToDiskAndThen(newState.persistentState, Accept()), ResetElectionTimeout())
    newState.currentTerm mustBe consensusState.currentTerm
    newState.log.size mustBe 3
    newState.log.getEntriesBetween(0, 3) mustBe entries
    newState.votedFor mustBe None
    newState.leaderHint mustBe Some(LEADER_ADDRESS)
    newState.volatileState mustBe VolatileState(0, TIMESTAMP)
    newState.state mustBe Follower()
  }

  @Test
  def appendAndCommitEntriesSameTerm(): Unit = {
    val appendEntries = AppendEntries(1, LEADER_ADDRESS, 0, 0, Seq(clientLogEntry(1)), 1, TIMESTAMP)
    val (newState, event) = StateTransitions.applyEvent(consensusState, appendEntries)
    event mustBe Set(SyncToDiskAndThen(newState.persistentState, Accept()),
      CommitEntries(consensusState.commitIndex, appendEntries.leaderCommit), ResetElectionTimeout())
    newState.currentTerm mustBe consensusState.currentTerm
    newState.log.size mustBe 1
    newState.log.getEntriesBetween(0, 1) mustBe appendEntries.entries
    newState.votedFor mustBe None
    newState.leaderHint mustBe Some(LEADER_ADDRESS)
    newState.volatileState mustBe VolatileState(1, TIMESTAMP)
    newState.state mustBe Follower()
  }

  @Test
  def cantAppendOrCommitSameTermMismatchedPrevLogIndex(): Unit = {
    val appendEntries = AppendEntries(1, LEADER_ADDRESS, 1, 0, Seq(clientLogEntry(1)), 0, TIMESTAMP)
    val (newState, event) = StateTransitions.applyEvent(consensusState, appendEntries)
    event mustBe Set(Reject(), ResetElectionTimeout())
    newState.persistentState mustBe consensusState.persistentState
    newState.volatileState mustBe VolatileState(consensusState.commitIndex, TIMESTAMP)
    newState.state mustBe consensusState.state
  }

  @Test
  def cantAppendOrCommitSameTermMismatchedPrevLogTerm(): Unit = {
    val appendEntries = AppendEntries(1, LEADER_ADDRESS, 1, 0, Seq(clientLogEntry(1, 1)), 1, TIMESTAMP)
    val stateWithLogEntry = consensusState.addEntry(clientLogEntry(1), 1)
    val (newState, event) = StateTransitions.applyEvent(stateWithLogEntry, appendEntries)
    event mustBe Set(Reject(), ResetElectionTimeout())
    newState.persistentState mustBe stateWithLogEntry.persistentState
    newState.volatileState mustBe VolatileState(stateWithLogEntry.commitIndex, TIMESTAMP)
    newState.state mustBe stateWithLogEntry.state
  }

  @Test
  def heartbeatWithUpdatedCommitIndexSameTerm(): Unit = {
    val appendEntries = AppendEntries(1, LEADER_ADDRESS, 1, 1, Seq(), 1, TIMESTAMP)
    val stateWithLogEntry = consensusState.addEntry(clientLogEntry(1), 1)
    val (newState, event) = StateTransitions.applyEvent(stateWithLogEntry, appendEntries)
    event mustBe Set(SyncToDiskAndThen(newState.persistentState, Accept()), CommitEntries(0, 1), ResetElectionTimeout())
    newState.currentTerm mustBe 1
    newState.log mustBe stateWithLogEntry.log
    newState.votedFor mustBe None
    newState.leaderHint mustBe Some(LEADER_ADDRESS)
    newState.volatileState mustBe VolatileState(1, TIMESTAMP)
    newState.state mustBe Follower()
  }

  @Test
  def correctlyUpdatesCommitIndex(): Unit = {
    val appendEntries = AppendEntries(1, LEADER_ADDRESS, 0, 0, Seq(clientLogEntry(1, 1)), 4, TIMESTAMP)
    val (newState, event) = StateTransitions.applyEvent(consensusState, appendEntries)
    event mustBe Set(SyncToDiskAndThen(newState.persistentState, Accept()), CommitEntries(0, 1), ResetElectionTimeout())
    newState.currentTerm mustBe 1
    newState.log.size mustBe 1
    newState.log.getEntriesBetween(0, 1) mustBe appendEntries.entries
    newState.votedFor mustBe None
    newState.leaderHint mustBe Some(LEADER_ADDRESS)
    newState.volatileState mustBe VolatileState(1, TIMESTAMP)
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
    newState.leaderHint mustBe Some(LEADER_ADDRESS)
    newState.votedFor mustBe None
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

  @Test
  def appendEntriesHigherTermClearsVote(): Unit = {
    val appendEntries = AppendEntries(2, LEADER_ADDRESS, 0, 0, Seq(), 0, TIMESTAMP)
    val stateWithVote = consensusState.voteFor(CANDIDATE_ADDRESS)
    stateWithVote.votedFor mustBe Some(CANDIDATE_ADDRESS)
    val (newState, event) = StateTransitions.applyEvent(stateWithVote, appendEntries)
    event mustBe Set(SyncToDiskAndThen(newState.persistentState, Accept()), ResetElectionTimeout())
    newState.currentTerm mustBe appendEntries.term
    newState.log mustBe stateWithVote.log
    newState.votedFor mustBe None
    newState.leaderHint mustBe Some(LEADER_ADDRESS)
    newState.volatileState mustBe VolatileState(0, TIMESTAMP)
    newState.state mustBe Follower()
  }

  @Test
  def appendEntriesSameTermDoesNotClearVote(): Unit = {
    val appendEntries = AppendEntries(consensusState.currentTerm, LEADER_ADDRESS, 0, 0, Seq(), 0, TIMESTAMP)
    val stateWithVote = consensusState.voteFor(CANDIDATE_ADDRESS)
    stateWithVote.votedFor mustBe Some(CANDIDATE_ADDRESS)
    val (newState, event) = StateTransitions.applyEvent(stateWithVote, appendEntries)
    event mustBe Set(SyncToDiskAndThen(newState.persistentState, Accept()), ResetElectionTimeout())
    newState.votedFor mustBe Some(CANDIDATE_ADDRESS)
    newState.currentTerm mustBe stateWithVote.currentTerm
    newState.log mustBe stateWithVote.log
    newState.votedFor mustBe stateWithVote.votedFor
    newState.leaderHint mustBe Some(LEADER_ADDRESS)
    newState.volatileState mustBe VolatileState(0, TIMESTAMP)
    newState.state mustBe Follower()
  }

  // req1 contains entries 1 and 2; req2 contains only entry 1 => log should have both entries
  @Test
  def appendEntriesOutOfOrder(): Unit = {
    val appendEntries1 = AppendEntries(consensusState.currentTerm, LEADER_ADDRESS, 0, 0, Seq(clientLogEntry(1), clientLogEntry(1, 1)), 0, TIMESTAMP)
    val (newState, event) = StateTransitions.applyEvent(consensusState, appendEntries1)
    event mustBe Set(SyncToDiskAndThen(newState.persistentState, Accept()), ResetElectionTimeout())
    newState.currentTerm mustBe consensusState.currentTerm
    newState.log.size mustBe 2
    newState.log.getEntriesBetween(0, 2) mustBe appendEntries1.entries
    newState.leaderHint mustBe Some(LEADER_ADDRESS)
    newState.votedFor mustBe None
    newState.volatileState mustBe VolatileState(0, TIMESTAMP)
    newState.state mustBe Follower()
    val appendEntries2 = AppendEntries(consensusState.currentTerm, LEADER_ADDRESS, 0, 0, Seq(clientLogEntry(1)), 0, TIMESTAMP)
    val (newState2, event2) = StateTransitions.applyEvent(newState, appendEntries2)
    event2 mustBe Set(Accept(), ResetElectionTimeout())
    newState2 mustBe newState
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
}
