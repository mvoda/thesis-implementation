package rx.distributed.raft

import com.google.protobuf.ByteString
import io.grpc.inprocess.InProcessServerBuilder
import org.apache.logging.log4j.scala.Logging
import org.scalatest.MustMatchers._
import rx.distributed.raft.LatchObservers.{OnCompletedCountdown, OnNextCountdown}
import rx.distributed.raft.consensus.config.{ClusterConfig, Config}
import rx.distributed.raft.consensus.server.Address.{ServerAddress, TestingAddress}
import rx.distributed.raft.consensus.server.ServerInfo.VotingServer
import rx.distributed.raft.consensus.state.RaftState.Follower
import rx.distributed.raft.consensus.state.{ConsensusState, LeaderState, PersistentState, VolatileState}
import rx.distributed.raft.events.Event
import rx.distributed.raft.events.client.{ClientCommandEvent, RegisterClientEvent}
import rx.distributed.raft.events.internal.{CountingAppendEntriesToServer, RequestVotesFromServer}
import rx.distributed.raft.events.rpc.request.{ClientRpcRequest, ConsensusRpcRequest}
import rx.distributed.raft.events.rpc.response._
import rx.distributed.raft.events.statemachine.{BecameLeader, CommitLogEntry}
import rx.distributed.raft.log.{ClientLogEntry, RaftLog}
import rx.distributed.raft.rpc.server.{CommandRPCServer, ConsensusRPCServer}
import rx.distributed.raft.statemachine.Statemachine
import rx.lang.scala.Notification.OnNext
import rx.lang.scala.observables.AsyncOnSubscribe
import rx.lang.scala.schedulers.TestScheduler
import rx.lang.scala.{Observable, Observer}

import scala.collection.immutable.HashSet
import scala.concurrent.duration.{Duration, MILLISECONDS}

object Utils extends Logging {
  private val DEFAULT_CONFIG = Config()
  private val EMPTY_LOG = RaftLog(testCluster())
  private val DEFAULT_VOLATILE_STATE = VolatileState()
  private val CLIENT_ID = "client"
  val TIMESTAMP: Long = 10000


  def createPersistentState(term: Int, votedFor: Option[ServerAddress]) = PersistentState(term, votedFor, EMPTY_LOG, None)

  def createPersistentState(term: Int, votedFor: Option[ServerAddress], clusterConfig: ClusterConfig) =
    PersistentState(term, votedFor, RaftLog(clusterConfig), None)

  def createFollower(term: Int, address: ServerAddress, clusterConfig: ClusterConfig): ConsensusState =
    ConsensusState(createPersistentState(term, None, clusterConfig), DEFAULT_VOLATILE_STATE, DEFAULT_CONFIG, address, Follower())

  def createFollower(term: Int, address: ServerAddress): ConsensusState = createFollower(term, address, testCluster())

  def createCandidate(term: Int, address: ServerAddress): ConsensusState = createFollower(term - 1, address).becomeCandidate()

  def createCandidate(term: Int, address: ServerAddress, clusterConfig: ClusterConfig): ConsensusState =
    createFollower(term - 1, address, clusterConfig).becomeCandidate()

  def createLeader(term: Int, address: ServerAddress): ConsensusState = createCandidate(term, address).becomeLeader()

  def createCluster(address: ServerAddress) = ClusterConfig(HashSet(VotingServer(address)))

  def defaultLeaderState(): LeaderState = LeaderState(EMPTY_LOG, testCluster())

  def emptyClientCommand(): ByteString = ByteString.copyFrom(new Array[Byte](0))

  def clientLogEntry(term: Int, seqNum: Int = 0, responseSeqNum: Int = 0): ClientLogEntry = {
    ClientLogEntry(term, ClientCommandEvent(CLIENT_ID, seqNum, responseSeqNum, TIMESTAMP, emptyClientCommand())(Observer()))
  }


  def testCluster(): ClusterConfig = ClusterConfig(HashSet(
    VotingServer(TestingAddress("test_server_1")),
    VotingServer(TestingAddress("test_server_2")),
    VotingServer(TestingAddress("test_server_3"))))

  def createConsensusServer(name: String): Observable[ConsensusRpcRequest] = {
    val clientServer = ConsensusRPCServer()
    Observable.create(AsyncOnSubscribe.singleState(() => InProcessServerBuilder.forName(name).addService(clientServer).build().start())
    ((_, _) => OnNext(clientServer.observable), (server) => {
      server.shutdownNow()
      server.awaitTermination()
    }))
  }

  def createCommandServer(name: String): Observable[ClientRpcRequest] = {
    val clientServer = CommandRPCServer()
    Observable.create(AsyncOnSubscribe.singleState(() => InProcessServerBuilder.forName(name).addService(clientServer).build().start())
    ((_, _) => OnNext(clientServer.observable), (server) => {
      server.shutdownNow()
      server.awaitTermination()
    }))
  }

  def createFsmCluster(addresses: Set[ServerAddress], config: Config, clusterConfig: ClusterConfig, statemachine: Statemachine): Map[ServerAddress, RaftFsm] = {
    addresses.map(address => (address, RaftFsm.synchronous(address, DEFAULT_CONFIG, clusterConfig, statemachine))).toMap
  }

  def triggerElectionTimeout(fsm: RaftFsm): Unit = {
    fsm.consensusModule.electionTimeoutSource.scheduler.asInstanceOf[TestScheduler].advanceTimeBy(Duration(DEFAULT_CONFIG.timeoutMax, MILLISECONDS))
  }

  def registerClient(fsm: RaftFsm): String = {
    val registerClientResponseObserver = OnCompletedCountdown[RegisterClientResponse](1)
    fsm.events.onNext(RegisterClientEvent(Seq(), System.currentTimeMillis())(registerClientResponseObserver.observer))
    registerClientResponseObserver.latch.await()
    registerClientResponseObserver.errors.size mustBe 0
    registerClientResponseObserver.events.size mustBe 1
    registerClientResponseObserver.events.head mustBe an[RegisterClientSuccessful]
    registerClientResponseObserver.events.head.asInstanceOf[RegisterClientSuccessful].clientId
  }

  def createClusterAssertElection(activeServers: Set[ServerAddress], clusterConfig: ClusterConfig,
                                  statemachine: Statemachine = TestStatemachine(DEFAULT_CONFIG.sessionTimeout)): Map[ServerAddress, RaftFsm] = {
    val followerCount = clusterConfig.size - 1
    val activeFollowerCount = activeServers.size - 1
    val expectedElectionEvents = 2 + 4 * activeFollowerCount + 2 * followerCount
    val electionObserver = OnNextCountdown[Event](expectedElectionEvents)
    val fsmCluster = Utils.createFsmCluster(activeServers, DEFAULT_CONFIG, clusterConfig, statemachine)
    val raftFsm = fsmCluster.head._2
    val electionSubscription = raftFsm.events.subscribe(electionObserver.observer)
    triggerElectionTimeout(raftFsm)
    electionObserver.latch.await()
    assertElectionEvents(electionObserver, followerCount, activeFollowerCount)
    electionSubscription.unsubscribe()
    fsmCluster
  }

  def assertElectionEvents(electionObserver: OnNextCountdown[Event], followerCount: Int, activeFollowerCount: Int): Unit = {
    electionObserver.errors.size mustBe 0
    electionObserver.events.size mustBe electionObserver.count
    electionObserver.events.count(_.isInstanceOf[CommitLogEntry]) mustBe 1
    electionObserver.events.count(_.isInstanceOf[BecameLeader]) mustBe 1
    // request votes from all servers
    electionObserver.events.count(_.isInstanceOf[RequestVotesFromServer]) mustBe followerCount
    // only active servers grant votes
    electionObserver.events.count(_.isInstanceOf[VoteGranted]) mustBe activeFollowerCount
    // send appendEntries to all servers (alive servers will reply with failure and leader will re-send appendEntries request)
    electionObserver.events.count(_.isInstanceOf[CountingAppendEntriesToServer]) mustBe followerCount + activeFollowerCount
    electionObserver.events.count(_.isInstanceOf[AppendEntriesFailure]) mustBe activeFollowerCount
    electionObserver.events.count(_.isInstanceOf[AppendEntriesSuccess]) mustBe activeFollowerCount
  }

}