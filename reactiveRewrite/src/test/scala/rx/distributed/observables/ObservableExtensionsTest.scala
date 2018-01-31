package rx.distributed.observables

import java.util.concurrent.CountDownLatch

import org.apache.logging.log4j.scala.Logging
import org.junit.{After, Before, Test}
import org.scalatest.MustMatchers._
import rx.distributed.observables.ObservableExtensions._
import rx.distributed.observables.RemoteObservableExtensions._
import rx.distributed.raft.LatchObservers.{OnCompletedCountdown, OnNextCountdown}
import rx.distributed.raft.Utils.assertElectionEvents
import rx.distributed.raft.consensus.PersistentStateIO
import rx.distributed.raft.consensus.config.{ClusterConfig, Config}
import rx.distributed.raft.consensus.server.Address.{ServerAddress, TestingAddress}
import rx.distributed.raft.consensus.server.ServerInfo.VotingServer
import rx.distributed.raft.events.Event
import rx.distributed.raft.statemachine.Statemachine
import rx.distributed.raft.{RaftFsm, TestStatemachine}
import rx.distributed.rpc.ClientServer
import rx.distributed.statemachine.RxStatemachine
import rx.lang.scala.Observable
import rx.lang.scala.schedulers.ImmediateScheduler

import scala.concurrent.duration.{Duration, SECONDS}
import scala.util.Try

class ObservableExtensionsTest extends Logging {
  private val ADDRESS = TestingAddress("test_server_1")
  private val CLIENT_ADDRESS = TestingAddress("client")
  private val CLUSTER_CONFIG = ClusterConfig(Set(VotingServer(ADDRESS)))
  private val DEFAULT_CONFIG = Config()
  private var fsmCluster: Map[ServerAddress, RaftFsm] = Map()
  ObservableExtensions.jarResolver = TestJarResolver
  RemoteObservableExtensions.jarResolver = TestJarResolver

  @Before
  def beforeEach(): Unit = {
    Try(PersistentStateIO(ADDRESS, ImmediateScheduler()).cleanup())
  }

  @Test
  def correctlyProcessesEvents(): Unit = {
    val streamEndpointObserver = OnCompletedCountdown[Int](1)
    fsmCluster = createAsyncClusterAssertElection(Set(ADDRESS), CLUSTER_CONFIG, RxStatemachine(30000))
    Observable.just(0, 1, 2, 3, 4, 5)
      .observeOnRemote(Set(ADDRESS)).map(_ + 1).filter(_ % 2 == 1)
      .observeOnLocal(CLIENT_ADDRESS)
      .subscribe(streamEndpointObserver.observer)
    streamEndpointObserver.latch.await()
    streamEndpointObserver.events mustBe Seq(1, 3, 5)
    streamEndpointObserver.errors.size mustBe 0
    // wait for the source observable to receive confirmation for all sent events before shutting down test;
    //    otherwise next tests will receive events from this test
    Thread.sleep(100)
  }

  @Test
  def pushesUnsubscribeToSource(): Unit = {
    var unsubscribed: Boolean = false
    val streamEndpointObserver = OnCompletedCountdown[Int](1)
    val unsubscribeLatch = new CountDownLatch(1)
    fsmCluster = createAsyncClusterAssertElection(Set(ADDRESS), CLUSTER_CONFIG, RxStatemachine(30000))
    Observable.interval(Duration(1, SECONDS)).map(_ => 1).doOnUnsubscribe {
      unsubscribed = true
      unsubscribeLatch.countDown()
    }.observeOnRemote(Set(ADDRESS)).map(_ * 10).take(1).observeOnLocal(CLIENT_ADDRESS)
      .subscribe(streamEndpointObserver.observer)
    streamEndpointObserver.latch.await()
    streamEndpointObserver.events mustBe Seq(10)
    streamEndpointObserver.errors.size mustBe 0
    unsubscribeLatch.await()
    unsubscribed mustBe true
    val rxStatemachine = fsmCluster(ADDRESS).statemachineModule.statemachine.asInstanceOf[RxStatemachine]
    rxStatemachine.streams mustBe Map()
    rxStatemachine.partialStreams mustBe Map()
    rxStatemachine.unsubscribeMessages mustBe Map()
  }

  @Test
  def chainedSimpleOperatorsSameCluster(): Unit = {
    var unsubscribed: Boolean = false
    val streamEndpointObserver = OnCompletedCountdown[Int](1)
    val unsubscribeLatch = new CountDownLatch(1)
    fsmCluster = createAsyncClusterAssertElection(Set(ADDRESS), CLUSTER_CONFIG, RxStatemachine(30000))
    Observable.interval(Duration(1, SECONDS)).map(_ => 1).doOnUnsubscribe {
      unsubscribed = true
      unsubscribeLatch.countDown()
    }.observeOnRemote(Set(ADDRESS)).map(_ * 10).take(3)
      .observeOnRemote(Set(ADDRESS)).map(_ * 10).take(1)
      .observeOnLocal(CLIENT_ADDRESS)
      .subscribe(streamEndpointObserver.observer)
    streamEndpointObserver.latch.await()
    streamEndpointObserver.events mustBe Seq(100)
    streamEndpointObserver.errors.size mustBe 0
    unsubscribeLatch.await()
    unsubscribed mustBe true
    val rxStatemachine = fsmCluster(ADDRESS).statemachineModule.statemachine.asInstanceOf[RxStatemachine]
    rxStatemachine.streams mustBe Map()
    rxStatemachine.partialStreams mustBe Map()
    rxStatemachine.unsubscribeMessages mustBe Map()
  }

  @Test
  def pushesCorrectResultsWithErrorInStream(): Unit = {
    var counter = 0
    var unsubscribed: Boolean = false
    val streamEndpointObserver = OnCompletedCountdown[Int](1)
    val unsubscribeLatch = new CountDownLatch(1)
    fsmCluster = createAsyncClusterAssertElection(Set(ADDRESS), CLUSTER_CONFIG, RxStatemachine(30000))
    Observable.interval(Duration(1, SECONDS)).map(_ => {
      counter = counter + 1
      counter
    }).doOnUnsubscribe {
      unsubscribed = true
      unsubscribeLatch.countDown()
    }.observeOnRemote(Set(ADDRESS)).map(_ * 10).flatMap(v => {
      if (v <= 20) {
        Observable.error(new Exception())
      } else {
        Observable.just(v)
      }
    }).take(1).retry().observeOnLocal(CLIENT_ADDRESS)
      .subscribe(streamEndpointObserver.observer)
    streamEndpointObserver.latch.await()
    streamEndpointObserver.events mustBe Seq(30)
    streamEndpointObserver.errors.size mustBe 0
    unsubscribeLatch.await()
    unsubscribed mustBe true
    val rxStatemachine = fsmCluster(ADDRESS).statemachineModule.statemachine.asInstanceOf[RxStatemachine]
    rxStatemachine.streams mustBe Map()
    rxStatemachine.partialStreams mustBe Map()
    rxStatemachine.unsubscribeMessages mustBe Map()
  }

  @After
  def afterEach(): Unit = {
    fsmCluster.values.foreach(_.shutdown())
    Try(PersistentStateIO(ADDRESS, ImmediateScheduler()).cleanup())
    Try(ClientServer.get(CLIENT_ADDRESS).shutdown())
    fsmCluster = Map()
  }

  private def createAsyncClusterAssertElection(activeServers: Set[ServerAddress], clusterConfig: ClusterConfig,
                                               statemachine: Statemachine = TestStatemachine(DEFAULT_CONFIG.sessionTimeout)): Map[ServerAddress, RaftFsm] = {
    val followerCount = clusterConfig.size - 1
    val activeFollowerCount = activeServers.size - 1
    val expectedElectionEvents = 2 + 4 * activeFollowerCount + 2 * followerCount
    val electionObserver = OnNextCountdown[Event](expectedElectionEvents)
    val fsmCluster = activeServers.map(address => (address, RaftFsm(address, DEFAULT_CONFIG, clusterConfig, statemachine))).toMap
    val raftFsm = fsmCluster.head._2
    val electionSubscription = raftFsm.events.subscribe(electionObserver.observer)
    electionObserver.latch.await()
    assertElectionEvents(electionObserver, followerCount, activeFollowerCount)
    electionSubscription.unsubscribe()
    fsmCluster
  }
}
