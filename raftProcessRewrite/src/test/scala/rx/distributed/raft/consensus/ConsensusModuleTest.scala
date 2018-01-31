package rx.distributed.raft.consensus

import org.junit.{After, Before, Test}
import org.scalatest.MustMatchers._
import rx.distributed.raft.consensus.config.Config
import rx.distributed.raft.consensus.server.Address.TestingAddress
import rx.distributed.raft.consensus.state.RaftState.{Candidate, Follower, Leader}
import rx.distributed.raft.events.Event
import rx.distributed.raft.events.client.RegisterClientEvent
import rx.distributed.raft.events.consensus._
import rx.distributed.raft.events.internal.{CountingAppendEntriesToServer, RequestVotesFromServer}
import rx.distributed.raft.events.rpc.request.{AppendEntriesRpcRequest, VoteRequestRpcRequest}
import rx.distributed.raft.events.rpc.response._
import rx.distributed.raft.events.statemachine.CommitLogEntry
import rx.distributed.raft.log.{LeaderNoOpEntry, NoOpEntry}
import rx.distributed.raft.timeout.RxTimeout
import rx.distributed.raft.{LatchObservers, Utils}
import rx.lang.scala.schedulers.{ImmediateScheduler, TestScheduler}
import rx.lang.scala.subjects.PublishSubject
import rx.lang.scala.{Observer, Subject}

import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.util.Try

class ConsensusModuleTest {
  private val ADDRESS = TestingAddress("test_server_1")
  private val ADDRESS_2 = TestingAddress("test_server_2")
  private val ADDRESS_3 = TestingAddress("test_server_3")
  private val NO_OP_ENTRY = NoOpEntry(0, 1000)
  private val CONFIG = Config()

  @Before
  def beforeEach(): Unit = {
    Try(PersistentStateIO(ADDRESS, ImmediateScheduler()).cleanup())
  }

  /**
    * Follower
    */

  @Test
  def handlesElectionTimeout(): Unit = {
    val consensusOutputObserver = LatchObservers.OnNextCountdown[Event](2)
    val followerConsensusModule = ConsensusModule.synchronous(ADDRESS, CONFIG, Utils.testCluster(), PublishSubject[Event]())
    followerConsensusModule.observable.subscribe(consensusOutputObserver.observer)
    followerConsensusModule.consensusState.state mustBe an[Follower]
    followerConsensusModule.handleInputEvent(ElectionTimeout())
    followerConsensusModule.consensusState.state mustBe an[Candidate]
    followerConsensusModule.consensusState.currentTerm mustBe 1
    followerConsensusModule.consensusState.votedFor mustBe Some(ADDRESS)
    consensusOutputObserver.latch.await()
    consensusOutputObserver.events.size mustBe 2
    val voteRequest = consensusOutputObserver.events.head.asInstanceOf[RequestVotesFromServer].voteRequest
    voteRequest.address mustBe ADDRESS
    voteRequest.term mustBe 1
    voteRequest.lastLogIndex mustBe 0
    voteRequest.lastLogTerm mustBe -1
    consensusOutputObserver.events.toSet mustBe Set(RequestVotesFromServer(ADDRESS_2, voteRequest), RequestVotesFromServer(ADDRESS_3, voteRequest))
  }

  @Test
  def appendsNoOpEntry(): Unit = {
    val latchObserver = LatchObservers.OnCompletedCountdown[AppendEntriesResponse](1)
    val followerConsensusModule = ConsensusModule(ADDRESS, CONFIG, Utils.testCluster(), PublishSubject[Event]())
    followerConsensusModule.consensusState.state mustBe an[Follower]
    followerConsensusModule.handleInputEvent(AppendEntriesRpcRequest(AppendEntries(0, ADDRESS_3, 0, -1, Seq(NO_OP_ENTRY), 0), latchObserver.observer))
    followerConsensusModule.consensusState.state mustBe an[Follower]
    followerConsensusModule.consensusState.log.size mustBe 1
    latchObserver.latch.await()
    latchObserver.events.size mustBe 1
    latchObserver.events.head mustBe AppendEntriesSuccess(0, ADDRESS, 1)
  }

  @Test
  def commitsNoOpEntry(): Unit = {
    val responseObserver = LatchObservers.OnCompletedCountdown[AppendEntriesResponse](1)
    val consensusOutputObserver = LatchObservers.OnNextCountdown[Event](1)
    val followerConsensusModule = ConsensusModule(ADDRESS, Config(), Utils.testCluster(), PublishSubject[Event]())
    followerConsensusModule.consensusState.state mustBe an[Follower]
    followerConsensusModule.observable.subscribe(consensusOutputObserver.observer)
    followerConsensusModule.handleInputEvent(AppendEntriesRpcRequest(AppendEntries(0, ADDRESS_3, 0, -1, Seq(NO_OP_ENTRY), 1), responseObserver.observer))
    followerConsensusModule.consensusState.state mustBe an[Follower]
    followerConsensusModule.consensusState.log.size mustBe 1
    responseObserver.latch.await()
    responseObserver.events.size mustBe 1
    responseObserver.events.head mustBe AppendEntriesSuccess(0, ADDRESS, 1)
    consensusOutputObserver.latch.await()
    consensusOutputObserver.events.size mustBe 1
    consensusOutputObserver.events.head mustBe CommitLogEntry(NO_OP_ENTRY, 1)
  }

  @Test
  def grantsVote(): Unit = {
    val latchObserver = LatchObservers.OnCompletedCountdown[VoteResponse](1)
    val followerConsensusModule = ConsensusModule(ADDRESS, CONFIG, Utils.testCluster(), PublishSubject[Event]())
    followerConsensusModule.consensusState.state mustBe an[Follower]
    followerConsensusModule.handleInputEvent(VoteRequestRpcRequest(VoteRequest(0, ADDRESS_3, 0, -1), latchObserver.observer))
    followerConsensusModule.consensusState.state mustBe an[Follower]
    followerConsensusModule.consensusState.log.size mustBe 0
    latchObserver.latch.await()
    latchObserver.events.size mustBe 1
    latchObserver.events.head mustBe VoteGranted(0, ADDRESS)
    followerConsensusModule.consensusState.votedFor mustBe Some(ADDRESS_3)
  }

  @Test
  def grantsVoteTwiceToSameCandidate(): Unit = {
    val firstResponseObserver = LatchObservers.OnCompletedCountdown[VoteResponse](1)
    val secondResponseObserver = LatchObservers.OnCompletedCountdown[VoteResponse](1)
    val followerConsensusModule = ConsensusModule(ADDRESS, CONFIG, Utils.testCluster(), PublishSubject[Event]())
    followerConsensusModule.consensusState.state mustBe an[Follower]
    followerConsensusModule.handleInputEvent(VoteRequestRpcRequest(VoteRequest(0, ADDRESS_3, 0, -1), firstResponseObserver.observer))
    followerConsensusModule.handleInputEvent(VoteRequestRpcRequest(VoteRequest(0, ADDRESS_3, 0, -1), secondResponseObserver.observer))
    followerConsensusModule.consensusState.state mustBe an[Follower]
    followerConsensusModule.consensusState.log.size mustBe 0
    firstResponseObserver.latch.await()
    secondResponseObserver.latch.await()
    firstResponseObserver.events mustBe secondResponseObserver.events
    followerConsensusModule.consensusState.votedFor mustBe Some(ADDRESS_3)
  }

  @Test
  def followerNotLeader(): Unit = {
    val responseObserver = LatchObservers.OnCompletedCountdown[RegisterClientResponse](1)
    val followerConsensusModule = ConsensusModule(ADDRESS, CONFIG, Utils.testCluster(), PublishSubject[Event]())
    followerConsensusModule.consensusState.state mustBe an[Follower]
    followerConsensusModule.handleInputEvent(RegisterClientEvent(Seq(), 1000)(responseObserver.observer))
    followerConsensusModule.consensusState.state mustBe an[Follower]
    followerConsensusModule.consensusState.log.size mustBe 0
    responseObserver.latch.await()
    responseObserver.events.size mustBe 1
    responseObserver.events.head mustBe RegisterClientNotLeader(None)
  }

  /**
    * Candidate
    */

  @Test
  def candidateReVotesForSelfAndRequestsVotes(): Unit = {
    val eventObserver = LatchObservers.OnNextCountdown[Event](2)
    val events = PublishSubject[Event]()
    val consensusModule = createCandidate(events)
    consensusModule.observable.subscribe(eventObserver.observer)
    consensusModule.handleInputEvent(ElectionTimeout())
    consensusModule.consensusState.state mustBe an[Candidate]
    consensusModule.consensusState.log.size mustBe 0
    consensusModule.consensusState.currentTerm mustBe 2
    val votes = consensusModule.consensusState.state.asInstanceOf[Candidate].votes
    votes.size mustBe 1
    votes.head mustBe VoteGranted(2, ADDRESS)
    eventObserver.latch.await()
    val voteRequest = eventObserver.events.head.asInstanceOf[RequestVotesFromServer].voteRequest
    voteRequest.address mustBe ADDRESS
    voteRequest.term mustBe 2
    voteRequest.lastLogIndex mustBe 0
    voteRequest.lastLogTerm mustBe -1
    eventObserver.events.toSet mustBe Set(RequestVotesFromServer(ADDRESS_2, voteRequest), RequestVotesFromServer(ADDRESS_3, voteRequest))
    events.onCompleted()
  }

  @Test
  def candidateNotLeader(): Unit = {
    val responseObserver = LatchObservers.OnCompletedCountdown[RegisterClientResponse](1)
    val events = PublishSubject[Event]()
    val candidateConsensusModule = createCandidate(events)
    candidateConsensusModule.consensusState.state mustBe an[Candidate]
    candidateConsensusModule.handleInputEvent(RegisterClientEvent(Seq(), 1000)(responseObserver.observer))
    candidateConsensusModule.consensusState.state mustBe an[Candidate]
    candidateConsensusModule.consensusState.log.size mustBe 0
    responseObserver.latch.await()
    responseObserver.events.size mustBe 1
    responseObserver.events.head mustBe RegisterClientNotLeader(None)
    events.onCompleted()
  }

  @Test
  def candidateBecomesLeaderSendsHeartbeatsAndAppendsNoOp(): Unit = {
    val eventObserver = LatchObservers.OnNextCountdown[Event](2)
    val events = PublishSubject[Event]()
    val consensusModule = createCandidate(events)
    consensusModule.consensusState.state mustBe an[Candidate]
    consensusModule.consensusState.log.size mustBe 0
    consensusModule.observable.subscribe(eventObserver.observer)
    consensusModule.handleInputEvent(VoteGranted(1, ADDRESS_2))
    consensusModule.consensusState.state mustBe an[Leader]
    consensusModule.consensusState.log.size mustBe 1
    consensusModule.consensusState.log.getEntriesAfter(0).head mustBe an[LeaderNoOpEntry]
    eventObserver.latch.await()
    eventObserver.events.size mustBe consensusModule.consensusState.log.clusterConfig.size - 1
    eventObserver.events.count(_.isInstanceOf[CountingAppendEntriesToServer]) mustBe consensusModule.consensusState.log.clusterConfig.size - 1
    events.onCompleted()
  }

  /**
    * Leader
    */

  @Test
  def leaderHandlesHeartbeatTimeout(): Unit = {
    val eventObserver = LatchObservers.OnNextCountdown[Event](2)
    val events = PublishSubject[Event]()
    val consensusModule = createLeader(events)
    val initState = consensusModule.consensusState
    initState.state mustBe an[Leader]
    consensusModule.observable.subscribe(eventObserver.observer)
    consensusModule.handleInputEvent(HeartbeatTimeout())
    consensusModule.consensusState mustBe initState
    eventObserver.latch.await()
    eventObserver.events.size mustBe consensusModule.consensusState.log.clusterConfig.size - 1
    eventObserver.events.count(_.isInstanceOf[CountingAppendEntriesToServer]) mustBe consensusModule.consensusState.log.clusterConfig.size - 1
    events.onCompleted()
  }

  @Test
  def leaderStepsDown(): Unit = {
    val events = PublishSubject[Event]()
    val consensusModule = createLeader(events)
    val initState = consensusModule.consensusState
    initState.state mustBe an[Leader]
    consensusModule.handleInputEvent(ElectionTimeout())
    consensusModule.consensusState.state mustBe an[Follower]
    consensusModule.consensusState.currentTerm mustBe initState.currentTerm
    events.onCompleted()
  }

  @Test
  def sendsAppendEntriesToClusterAfterClientEventAndAppendsEntriesToSelf(): Unit = {
    val eventObserver = LatchObservers.OnNextCountdown[Event](2)
    val events = PublishSubject[Event]()
    val consensusModule = createLeader(events)
    val initState = consensusModule.consensusState
    initState.state mustBe an[Leader]
    consensusModule.observable.subscribe(eventObserver.observer)
    consensusModule.consensusState.state.asInstanceOf[Leader].leaderState.matchIndex(ADDRESS) mustBe 1
    consensusModule.handleInputEvent(RegisterClientEvent(Seq(), 1000)(Observer()))
    consensusModule.consensusState.log.size mustBe initState.log.size + 1
    val leaderState = consensusModule.consensusState.state.asInstanceOf[Leader].leaderState
    leaderState.matchIndex(ADDRESS) mustBe 2
    eventObserver.latch.await()
    eventObserver.events.size mustBe consensusModule.consensusState.log.clusterConfig.size - 1
    eventObserver.events.count(_.isInstanceOf[CountingAppendEntriesToServer]) mustBe consensusModule.consensusState.log.clusterConfig.size - 1
    events.onCompleted()
  }

  @Test
  def doesNotStepDownAfterReachingMajority(): Unit = {
    val eventObserver = LatchObservers.OnNextCountdown[Event](2)
    val events = PublishSubject[Event]()
    val consensusModule = createTestLeader(events, Config(1000, 1000))
    val initState = consensusModule.consensusState
    initState.state mustBe an[Leader]
    consensusModule.observable.subscribe(eventObserver.observer)
    //TODO: remove this once election timeouts are started by default
    consensusModule.electionTimeoutSource.start()
    consensusModule.handleInputEvent(HeartbeatTimeout())
    eventObserver.latch.await()
    eventObserver.events.size mustBe consensusModule.consensusState.log.clusterConfig.size - 1
    eventObserver.events.count(_.isInstanceOf[CountingAppendEntriesToServer]) mustBe consensusModule.consensusState.log.clusterConfig.size - 1
    consensusModule.electionTimeoutSource.scheduler.asInstanceOf[TestScheduler].advanceTimeBy(Duration(500, MILLISECONDS))
    eventObserver.events.head.asInstanceOf[CountingAppendEntriesToServer].activeLeaderObserver.foreach(obs => obs.onNext(AppendEntriesSuccess(1, ADDRESS_2, 1)))
    consensusModule.electionTimeoutSource.scheduler.asInstanceOf[TestScheduler].advanceTimeBy(Duration(900, MILLISECONDS))
    consensusModule.consensusState mustBe initState
    events.onCompleted()
  }

  @Test
  def stepsDownAfterFailingToReachMajority(): Unit = {
    val eventObserver = LatchObservers.OnNextCountdown[Event](2)
    val events = PublishSubject[Event]()
    val consensusModule = createTestLeader(events, Config(1000, 1000))
    val initState = consensusModule.consensusState
    initState.state mustBe an[Leader]
    consensusModule.observable.subscribe(eventObserver.observer)
    //TODO: remove this once election timeouts are started by default
    consensusModule.electionTimeoutSource.start()
    consensusModule.handleInputEvent(HeartbeatTimeout())
    eventObserver.latch.await()
    eventObserver.events.size mustBe consensusModule.consensusState.log.clusterConfig.size - 1
    eventObserver.events.count(_.isInstanceOf[CountingAppendEntriesToServer]) mustBe consensusModule.consensusState.log.clusterConfig.size - 1
    consensusModule.electionTimeoutSource.scheduler.asInstanceOf[TestScheduler].advanceTimeBy(Duration(1000, MILLISECONDS))
    consensusModule.consensusState.persistentState mustBe initState.persistentState
    consensusModule.consensusState.state mustBe an[Follower]
    events.onCompleted()
  }

  @Test
  def sendsNotLeaderResponseWhenSteppingDown(): Unit = {
    val responseObserver = LatchObservers.OnCompletedCountdown[RegisterClientResponse](1)
    val events = PublishSubject[Event]()
    val consensusModule = createTestLeader(events, Config(1000, 1000))
    val initState = consensusModule.consensusState
    initState.state mustBe an[Leader]
    //TODO: remove this once election timeouts are started by default
    consensusModule.electionTimeoutSource.start()
    consensusModule.handleInputEvent(RegisterClientEvent(Seq(), 1000)(responseObserver.observer))
    consensusModule.electionTimeoutSource.scheduler.asInstanceOf[TestScheduler].advanceTimeBy(Duration(1000, MILLISECONDS))
    responseObserver.latch.await()
    responseObserver.events.size mustBe 1
    responseObserver.events.head mustBe RegisterClientNotLeader(None)
    events.onCompleted()
  }

  @After
  def afterEach(): Unit = {
    Try(PersistentStateIO(ADDRESS, ImmediateScheduler()).cleanup())
  }

  private def createCandidate(events: Subject[Event]): ConsensusModule = {
    val module = ConsensusModule.synchronous(ADDRESS, CONFIG, Utils.testCluster(), events)
    module.handleInputEvent(ElectionTimeout())
    module
  }

  private def createLeader(events: Subject[Event]): ConsensusModule = {
    val module = createCandidate(events)
    module.handleInputEvent(VoteGranted(1, ADDRESS_2))
    module
  }

  private def createTestLeader(events: Subject[Event], config: Config): ConsensusModule = {
    val module = ConsensusModule(ADDRESS, config, Utils.testCluster(), events, RxTimeout(config, TestScheduler()), RxTimeout(config.heartbeatTimeout), ImmediateScheduler(), PersistentStateIO(ADDRESS, ImmediateScheduler()))
    module.handleInputEvent(ElectionTimeout())
    module.handleInputEvent(VoteGranted(1, ADDRESS_2))
    module
  }


}
