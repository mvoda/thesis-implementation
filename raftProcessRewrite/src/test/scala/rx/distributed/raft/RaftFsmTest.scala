package rx.distributed.raft

import org.apache.logging.log4j.scala.Logging
import org.junit.{After, Before, Test}
import org.scalatest.MustMatchers._
import rx.distributed.raft.LatchObservers.OnNextCountdown
import rx.distributed.raft.consensus.PersistentStateIO
import rx.distributed.raft.consensus.config.{ClusterConfig, Config}
import rx.distributed.raft.consensus.server.Address.{ServerAddress, TestingAddress}
import rx.distributed.raft.consensus.server.ServerInfo.{ServerType, VotingServer}
import rx.distributed.raft.consensus.state.RaftState.Leader
import rx.distributed.raft.events.Event
import rx.distributed.raft.events.client.{ExpireSessions, RegisterClientEvent}
import rx.distributed.raft.events.internal.{CountingAppendEntriesToServer, RequestVotesFromServer}
import rx.distributed.raft.events.rpc.response._
import rx.distributed.raft.events.statemachine.CommitLogEntry
import rx.distributed.raft.log.{LeaderNoOpEntry, NoOpEntry}
import rx.lang.scala.schedulers.ImmediateScheduler

import scala.collection.immutable.HashSet
import scala.util.Try

class RaftFsmTest extends Logging {
  private val ADDRESS = TestingAddress("test_server_1")
  private val ADDRESS2 = TestingAddress("test_server_2")
  private val ADDRESS3 = TestingAddress("test_server_3")
  private val CLUSTER_CONFIG = ClusterConfig(HashSet(VotingServer(ADDRESS), VotingServer(ADDRESS2), VotingServer(ADDRESS3)))
  private val DEFAULT_CONFIG = Config()
  private var fsmCluster: Map[ServerAddress, RaftFsm] = Map()

  @Before
  def beforeEach(): Unit = {
    Try(PersistentStateIO(ADDRESS, ImmediateScheduler()).cleanup())
    Try(PersistentStateIO(ADDRESS2, ImmediateScheduler()).cleanup())
    Try(PersistentStateIO(ADDRESS3, ImmediateScheduler()).cleanup())
  }

  @Test
  def sendsCorrectEventsOnLeaderTransitionFull1ServerCluster(): Unit = {
    val clusterConfig = ClusterConfig(Set(VotingServer(ADDRESS)))
    fsmCluster = Utils.createClusterAssertElection(Set(ADDRESS), clusterConfig)
  }

  @Test
  def sendsCorrectEventsOnLeaderTransitionFull3ServerCluster(): Unit = {
    fsmCluster = Utils.createClusterAssertElection(Set(ADDRESS, ADDRESS2, ADDRESS3), CLUSTER_CONFIG)
    assertLogsAreReplicated()
  }

  @Test
  def sendsCorrectEventsOnLeaderTransitionPartial2Of3ServerCluster(): Unit = {
    fsmCluster = Utils.createClusterAssertElection(Set(ADDRESS, ADDRESS2), CLUSTER_CONFIG)
    assertLogsAreReplicated()
  }

  @Test
  def sendsCorrectEventsOnLeaderTransitionPartial3Of5ServerCluster(): Unit = {
    val votingServers = Set(ADDRESS, ADDRESS2, ADDRESS3, TestingAddress("test_server_4"), TestingAddress("test_server_5"))
      .map(VotingServer(_).asInstanceOf[ServerType])
    val clusterConfig = ClusterConfig(votingServers)
    fsmCluster = Utils.createClusterAssertElection(Set(ADDRESS, ADDRESS2, ADDRESS3), clusterConfig)
    assertLogsAreReplicated()
  }

  @Test
  def replicatesRegisterClientFull3ServerCluster(): Unit = {
    val logReplicationObserver = OnNextCountdown[Event](6)
    fsmCluster = Utils.createClusterAssertElection(Set(ADDRESS, ADDRESS2, ADDRESS3), CLUSTER_CONFIG)
    val raftFsm = fsmCluster(ADDRESS)
    raftFsm.events.subscribe(logReplicationObserver.observer)
    Utils.registerClient(raftFsm)
    logReplicationObserver.latch.await()
    logReplicationObserver.errors.size mustBe 0
    logReplicationObserver.events.size mustBe logReplicationObserver.count
    logReplicationObserver.events.count(_.isInstanceOf[RegisterClientEvent]) mustBe 1
    logReplicationObserver.events.count(_.isInstanceOf[CountingAppendEntriesToServer]) mustBe 2
    logReplicationObserver.events.count(_.isInstanceOf[AppendEntriesSuccess]) mustBe 2
    logReplicationObserver.events.count(_.isInstanceOf[CommitLogEntry]) mustBe 1
    assertLogsAreReplicated()
  }

  @Test
  def expiresSessionsAfterClientInactivity(): Unit = {
    val logReplicationObserver = OnNextCountdown[Event](6)
    val expireSessionsObserver = OnNextCountdown[Event](1)
    val commitExpiredSessionsObserver = OnNextCountdown[Event](1)
    fsmCluster = Utils.createClusterAssertElection(Set(ADDRESS, ADDRESS2, ADDRESS3), CLUSTER_CONFIG, TestStatemachine(2000))
    val raftFsm = fsmCluster(ADDRESS)
    raftFsm.events.subscribe(logReplicationObserver.observer)
    Utils.registerClient(raftFsm)
    logReplicationObserver.latch.await()
    raftFsm.events.collect { case event: ExpireSessions => event }.subscribe(expireSessionsObserver.observer)
    raftFsm.events.collect { case event: CommitLogEntry => event }.subscribe(commitExpiredSessionsObserver.observer)
    expireSessionsObserver.latch.await()
    expireSessionsObserver.errors.size mustBe 0
    expireSessionsObserver.events.size mustBe 1
    commitExpiredSessionsObserver.latch.await()
    commitExpiredSessionsObserver.errors.size mustBe 0
    commitExpiredSessionsObserver.events.size mustBe 1
    assertLogsAreReplicated()
  }

  @Test
  def becomesFollowerThenCandidateWhenCantReachMajority(): Unit = {
    val electionObserver = OnNextCountdown[Event](14)
    val voteRequestsObserver = OnNextCountdown[RequestVotesFromServer](2)
    fsmCluster = Utils.createFsmCluster(Set(ADDRESS2, ADDRESS3), Config(251, 500), CLUSTER_CONFIG, TestStatemachine(DEFAULT_CONFIG.sessionTimeout))
    val raftFsm = RaftFsm(ADDRESS, Config(251, 500), CLUSTER_CONFIG, TestStatemachine(DEFAULT_CONFIG.sessionTimeout))
    val electionSub = raftFsm.events.subscribe(electionObserver.observer)
    electionObserver.latch.await()
    Utils.assertElectionEvents(electionObserver, 2, 2)
    raftFsm.consensusModule.consensusState.state mustBe an[Leader]
    electionSub.unsubscribe()
    fsmCluster.values.foreach(_.shutdown())
    fsmCluster = fsmCluster + (ADDRESS -> raftFsm)
    raftFsm.events.collect { case request: RequestVotesFromServer => request }.subscribe(voteRequestsObserver.observer)
    voteRequestsObserver.latch.await()
    voteRequestsObserver.errors.size mustBe 0
    voteRequestsObserver.events.size mustBe voteRequestsObserver.count
    voteRequestsObserver.events.map(_.server).toSet mustBe Set(ADDRESS2, ADDRESS3)
    voteRequestsObserver.events.map(_.voteRequest.term).toSet mustBe Set(2, 2)
  }

  private def assertLogsAreReplicated(): Unit = {
    val logEntries = fsmCluster.head._2.consensusModule.consensusState.log.allEntries().map {
      case LeaderNoOpEntry(term, timestamp) => NoOpEntry(term, timestamp)
      case entry => entry
    }
    fsmCluster.tail.values.foreach(fsm => fsm.consensusModule.consensusState.log.allEntries() mustBe logEntries)
  }

  @After
  def afterEach(): Unit = {
    fsmCluster.values.foreach(_.shutdown())
    Try(PersistentStateIO(ADDRESS, ImmediateScheduler()).cleanup())
    Try(PersistentStateIO(ADDRESS2, ImmediateScheduler()).cleanup())
    Try(PersistentStateIO(ADDRESS3, ImmediateScheduler()).cleanup())
    fsmCluster = Map()
  }

  //TODO: --- tests ---
  //TODO:   FORWARDING MODULE STEPPED DOWN
  //TODO:   log replication (partial cluster)
  //TODO:   log replication with different terms
  //TODO:   client command
  //TODO:   client keepAlive
  //TODO:   remove commands
  //TODO:   notifies when server was elected leader (e.g. to start forwarding module)

}
