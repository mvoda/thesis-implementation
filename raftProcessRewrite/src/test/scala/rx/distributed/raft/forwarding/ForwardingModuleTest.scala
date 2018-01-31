package rx.distributed.raft.forwarding

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.logging.log4j.scala.Logging
import org.junit.{After, Before, Test}
import org.scalatest.MustMatchers.{an, _}
import rx.distributed.raft.LatchObservers.{OnCompletedCountdown, OnNextCountdown}
import rx.distributed.raft._
import rx.distributed.raft.client.RaftClusterClient
import rx.distributed.raft.consensus.PersistentStateIO
import rx.distributed.raft.consensus.config.{ClusterConfig, Config}
import rx.distributed.raft.consensus.server.Address.{ServerAddress, TestingAddress}
import rx.distributed.raft.consensus.server.ServerInfo.VotingServer
import rx.distributed.raft.events.Event
import rx.distributed.raft.events.client.{ClientCommandEvent, ExpireSessions}
import rx.distributed.raft.events.consensus.ElectionTimeout
import rx.distributed.raft.events.forwarding.{ForwardingModuleEvent, LeaderSteppedDown, RemoveCommand}
import rx.distributed.raft.events.rpc.request.ClientRpcRequest
import rx.distributed.raft.events.statemachine.{BecameLeader, DownstreamClientExpired, RegisterDownstreamClient, SendDownstream}
import rx.distributed.raft.rpc.client.RaftClient
import rx.distributed.raft.statemachine.output.StatemachineResponse
import rx.distributed.raft.statemachine.session.{ClientProducer, CommandSessions, RaftClusterProducer}
import rx.lang.scala.Observable
import rx.lang.scala.schedulers.ImmediateScheduler
import rx.lang.scala.subjects.PublishSubject

import scala.concurrent.duration.{Duration, SECONDS}
import scala.util.Try

class ForwardingModuleTest extends Logging {
  private val ADDRESS = TestingAddress("test_server_1")
  private val ADDRESS2 = TestingAddress("test_server_2")
  private val ADDRESS3 = TestingAddress("test_server_3")
  private var fsmCluster: Map[ServerAddress, RaftFsm] = Map()

  @Before
  def beforeEach(): Unit = {
    Try(PersistentStateIO(ADDRESS, ImmediateScheduler()).cleanup())
    Try(PersistentStateIO(ADDRESS2, ImmediateScheduler()).cleanup())
    Try(PersistentStateIO(ADDRESS3, ImmediateScheduler()).cleanup())
  }

  //TODO:   add test for stepped down

  @Test
  def sendsCommandsDownstreamAndProducesRemoveCommandEvents(): Unit = {
    val clusterConfig = ClusterConfig(Set(VotingServer(ADDRESS2)))
    val downstreamEventObserver = OnNextCountdown[ClientRpcRequest](2)
    val forwardingOutputObserver = OnNextCountdown[Event](2)
    val events = PublishSubject[Event]()
    val forwardingModule = ForwardingModule(ADDRESS, events, 5, 1)
    forwardingModule.observable.subscribe(forwardingOutputObserver.observer)
    fsmCluster = Utils.createClusterAssertElection(Set(ADDRESS2), clusterConfig)
    val raftFsm = fsmCluster(ADDRESS2)
    raftFsm.events.collect { case clientCommand: ClientCommandEvent => clientCommand }.subscribe(downstreamEventObserver.observer)
    val clientId = Utils.registerClient(raftFsm)
    events.onNext(BecameLeader(CommandSessions()))
    events.onNext(SendDownstream(RaftClusterClient(Set(ADDRESS2), clientId), Write(1, 10)))
    Observable.just(SendDownstream(RaftClusterClient(Set(ADDRESS2), clientId), Write(2, 20)))
      .delay(Duration(2, SECONDS)).subscribe(command => events.onNext(command))
    downstreamEventObserver.latch.await()
    downstreamEventObserver.events.size mustBe 2
    downstreamEventObserver.events.head mustBe an[ClientCommandEvent]
    forwardingOutputObserver.latch.await(10, TimeUnit.SECONDS)
    forwardingOutputObserver.events.size mustBe 2
    forwardingOutputObserver.events.head mustBe an[RemoveCommand]
    forwardingOutputObserver.events.head.asInstanceOf[RemoveCommand].seqNum mustBe 0
    forwardingOutputObserver.events(1).asInstanceOf[RemoveCommand].seqNum mustBe 1
    forwardingOutputObserver.errors.size mustBe 0
    events.onNext(DownstreamClientExpired(RaftClusterClient(Set(ADDRESS2), clientId)))
    events.onCompleted()
  }

  @Test
  def sessionsExpireOnDownstream(): Unit = {
    val commandResponseObserver = OnCompletedCountdown[StatemachineResponse](1)
    val expireSessionsObserver = OnNextCountdown[Event](1)
    val upstream = RaftFsm(ADDRESS, Config(251, 500), ClusterConfig(Set(VotingServer(ADDRESS))), TestStatemachine(3000))
    val downstream = RaftFsm(ADDRESS2, Config(251, 500), ClusterConfig(Set(VotingServer(ADDRESS2))), TestStatemachine(3000))
    fsmCluster = Map(ADDRESS -> upstream, ADDRESS2 -> downstream)
    val upstreamLatch = electionLatch(upstream)
    val downstreamLatch = electionLatch(downstream)
    upstreamLatch.await()
    downstreamLatch.await()
    val client = RaftClient(Set(ADDRESS), 5, 1, Seq())
    client.sendCommands(Observable.just(DownstreamWrite(1, 1, Set(ADDRESS2))))
      .subscribe(commandResponseObserver.observer)
    commandResponseObserver.latch.await()
    commandResponseObserver.errors.size mustBe 0
    commandResponseObserver.events.size mustBe commandResponseObserver.count
    commandResponseObserver.events.head mustBe Empty()
    downstream.events.collect { case event: ExpireSessions => event }.subscribe(expireSessionsObserver.observer)
    expireSessionsObserver.latch.await()
    expireSessionsObserver.errors.size mustBe 0
    expireSessionsObserver.events.size mustBe 1
  }

  @Test
  def forwardingModuleReceivesSteppedDownAndBecameLeader(): Unit = {
    var forwardingEvents = Seq[ForwardingModuleEvent]()
    val commandResponseObserver = OnCompletedCountdown[StatemachineResponse](1)
    val server = RaftFsm(ADDRESS, Config(251, 500), ClusterConfig(Set(VotingServer(ADDRESS))), TestStatemachine(3000))
    fsmCluster = Map(ADDRESS -> server)
    val upstreamLatch = electionLatch(server)
    upstreamLatch.await()
    val client = RaftClient(Set(ADDRESS), 5, 1, Seq())
    client.sendCommands(Observable.just(DownstreamWrite(1, 1, Set(ADDRESS2))))
      .subscribe(commandResponseObserver.observer)
    server.events.collect { case e: ForwardingModuleEvent => e }.subscribe(event => forwardingEvents = forwardingEvents :+ event)
    Thread.sleep(1000)
    forwardingEvents.size mustBe 1
    forwardingEvents.head mustBe RegisterDownstreamClient(RaftClusterProducer(Set(ADDRESS2), ClientProducer("2", Seq())))
    server.consensusModule.handleInputEvent(ElectionTimeout())
    Thread.sleep(1000)
    forwardingEvents.size mustBe 3
    forwardingEvents(1) mustBe LeaderSteppedDown()
    forwardingEvents(2) mustBe an[BecameLeader]
  }

  @After
  def afterEach(): Unit = {
    fsmCluster.values.foreach(_.shutdown())
    Try(PersistentStateIO(ADDRESS, ImmediateScheduler()).cleanup())
    Try(PersistentStateIO(ADDRESS2, ImmediateScheduler()).cleanup())
    Try(PersistentStateIO(ADDRESS3, ImmediateScheduler()).cleanup())
    fsmCluster = Map()
  }

  private def electionLatch(fsm: RaftFsm): CountDownLatch = {
    val electionObserver = OnNextCountdown[Event](2)
    fsm.events.subscribe(electionObserver.observer)
    electionObserver.latch
  }
}
