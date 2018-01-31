package rx.distributed.raft.statemachine

import org.apache.logging.log4j.scala.Logging
import org.junit.Test
import org.scalatest.MustMatchers._
import rx.distributed.raft.client.RaftClusterClient
import rx.distributed.raft.consensus.config.ClusterConfig
import rx.distributed.raft.consensus.server.Address.{ServerAddress, TestingAddress}
import rx.distributed.raft.consensus.server.ServerInfo.VotingServer
import rx.distributed.raft.consensus.state.PersistentState
import rx.distributed.raft.events.client.{ClientCommandEvent, RegisterClientEvent}
import rx.distributed.raft.events.statemachine._
import rx.distributed.raft.log._
import rx.distributed.raft.statemachine.input.StatemachineCommand
import rx.distributed.raft.statemachine.session.{ClientProducer, RaftClusterProducer}
import rx.distributed.raft.util.ByteStringSerializer
import rx.distributed.raft.{DownstreamWrite, TestStatemachine, Write}
import rx.lang.scala.Observer
import rx.lang.scala.schedulers.ImmediateScheduler
import rx.lang.scala.subjects.PublishSubject

class StatemachineModuleTest extends Logging {
  private val ADDRESS = TestingAddress("test_server_1")
  private val TIMESTAMP = 1000
  private val CLUSTER_CONFIG = ClusterConfig(Set(VotingServer(ADDRESS)))
  private val CLIENT_ID = "client"
  private val DOWNSTREAM_CLUSTER = Set(TestingAddress("test_server_2").asInstanceOf[ServerAddress])
  private val SESSION_ID = "downstream_client"
  private val registerClientEntry = CommitLogEntry(RegisterClientEntry(1, CLIENT_ID, RegisterClientEvent(Seq(), TIMESTAMP)(Observer())), 1)
  private val DOWNSTREAM_SESSION_CLIENT = RaftClusterClient(DOWNSTREAM_CLUSTER, SESSION_ID)
  private val DOWNSTREAM_PRODUCER = RaftClusterProducer(DOWNSTREAM_CLUSTER, ClientProducer(CLIENT_ID, Seq()))


  @Test
  def regularCommandDoesNotProduceAnything(): Unit = {
    val eventSubject = PublishSubject[CommitLogEntry]()
    var statemachineOutput = Seq[StatemachineOutputEvent]()
    val statemachineModule = StatemachineModule(ADDRESS, TestStatemachine(100), eventSubject, ImmediateScheduler())
    statemachineModule.observable.subscribe(event => statemachineOutput = statemachineOutput :+ event, e => throw e)
    eventSubject.onNext(registerClientEntry)
    eventSubject.onNext(commitEntryEvent(0, Write(10, 10), 1010))
    statemachineOutput.size mustBe 0
  }

  @Test
  def downstreamFirstCommandDoesProducesRegister(): Unit = {
    val eventSubject = PublishSubject[CommitLogEntry]()
    var statemachineOutput = Seq[StatemachineOutputEvent]()
    val statemachineModule = StatemachineModule(ADDRESS, TestStatemachine(100), eventSubject, ImmediateScheduler())
    statemachineModule.observable.subscribe(event => statemachineOutput = statemachineOutput :+ event, e => throw e)
    eventSubject.onNext(registerClientEntry)
    eventSubject.onNext(commitEntryEvent(0, DownstreamWrite(10, 10, DOWNSTREAM_CLUSTER), 1010))
    statemachineOutput.size mustBe 1
    statemachineOutput.head mustBe RegisterDownstreamClient(DOWNSTREAM_PRODUCER)
  }

  @Test
  def secondCommandDoesNotProduceAnythingIfNotRegistered(): Unit = {
    val eventSubject = PublishSubject[CommitLogEntry]()
    var statemachineOutput = Seq[StatemachineOutputEvent]()
    val statemachineModule = StatemachineModule(ADDRESS, TestStatemachine(100), eventSubject, ImmediateScheduler())
    statemachineModule.observable.subscribe(event => statemachineOutput = statemachineOutput :+ event, e => throw e)
    eventSubject.onNext(registerClientEntry)
    eventSubject.onNext(commitEntryEvent(0, DownstreamWrite(10, 10, DOWNSTREAM_CLUSTER), 1020))
    eventSubject.onNext(commitEntryEvent(1, DownstreamWrite(20, 20, DOWNSTREAM_CLUSTER), 1030))
    logger.info(statemachineOutput)
    statemachineOutput.size mustBe 1
    statemachineOutput.head mustBe RegisterDownstreamClient(DOWNSTREAM_PRODUCER)
  }

  @Test
  def registeredDownstreamEntryPushesStoredCommands(): Unit = {
    val registeredId = "1"
    val eventSubject = PublishSubject[CommitLogEntry]()
    var statemachineOutput = Seq[StatemachineOutputEvent]()
    val statemachineModule = StatemachineModule(ADDRESS, TestStatemachine(100), eventSubject, ImmediateScheduler())
    statemachineModule.observable.subscribe(event => statemachineOutput = statemachineOutput :+ event, e => throw e)
    eventSubject.onNext(registerClientEntry)
    eventSubject.onNext(commitEntryEvent(0, DownstreamWrite(10, 10, DOWNSTREAM_CLUSTER), 1020))
    eventSubject.onNext(commitEntryEvent(1, DownstreamWrite(20, 20, DOWNSTREAM_CLUSTER), 1030))
    statemachineOutput.size mustBe 1
    statemachineOutput.head mustBe RegisterDownstreamClient(DOWNSTREAM_PRODUCER)
    eventSubject.onNext(CommitLogEntry(RegisteredDownstreamClientEntry(1, DOWNSTREAM_PRODUCER, registeredId, 1040), 3))
    statemachineOutput.size mustBe 3
    statemachineOutput(1) mustBe SendDownstream(RaftClusterClient(DOWNSTREAM_CLUSTER, registeredId), Write(10, 100))
    statemachineOutput(2) mustBe SendDownstream(RaftClusterClient(DOWNSTREAM_CLUSTER, registeredId), Write(20, 200))
  }

  @Test
  def commandsAfterRegisteredClientPushCommands(): Unit = {
    val registeredId = "1"
    val eventSubject = PublishSubject[CommitLogEntry]()
    var statemachineOutput = Seq[StatemachineOutputEvent]()
    val statemachineModule = StatemachineModule(ADDRESS, TestStatemachine(100), eventSubject, ImmediateScheduler())
    statemachineModule.observable.subscribe(event => statemachineOutput = statemachineOutput :+ event, e => throw e)
    eventSubject.onNext(registerClientEntry)
    eventSubject.onNext(commitEntryEvent(0, DownstreamWrite(10, 10, DOWNSTREAM_CLUSTER), 1020))
    statemachineOutput.size mustBe 1
    statemachineOutput.head mustBe RegisterDownstreamClient(DOWNSTREAM_PRODUCER)
    eventSubject.onNext(CommitLogEntry(RegisteredDownstreamClientEntry(1, DOWNSTREAM_PRODUCER, registeredId, 1040), 3))
    statemachineOutput.size mustBe 2
    statemachineOutput(1) mustBe SendDownstream(RaftClusterClient(DOWNSTREAM_CLUSTER, registeredId), Write(10, 100))
    eventSubject.onNext(commitEntryEvent(1, DownstreamWrite(20, 20, DOWNSTREAM_CLUSTER), 1030))
    statemachineOutput.size mustBe 3
    statemachineOutput(2) mustBe SendDownstream(RaftClusterClient(DOWNSTREAM_CLUSTER, registeredId), Write(20, 200))
  }

  @Test
  def removesCorrectExpiredClient(): Unit = {
    val registeredId = "1"
    val eventSubject = PublishSubject[CommitLogEntry]()
    val registeredDownstreamClient = RaftClusterClient(DOWNSTREAM_CLUSTER, registeredId)
    var statemachineOutput = Seq[StatemachineOutputEvent]()
    val statemachineModule = StatemachineModule(ADDRESS, TestStatemachine(100), eventSubject, ImmediateScheduler())
    statemachineModule.observable.subscribe(event => statemachineOutput = statemachineOutput :+ event, e => throw e)
    eventSubject.onNext(registerClientEntry)
    eventSubject.onNext(commitEntryEvent(0, DownstreamWrite(10, 10, DOWNSTREAM_CLUSTER), 1020))
    statemachineOutput.size mustBe 1
    statemachineOutput.head mustBe RegisterDownstreamClient(DOWNSTREAM_PRODUCER)
    eventSubject.onNext(CommitLogEntry(RegisteredDownstreamClientEntry(1, DOWNSTREAM_PRODUCER, registeredId, 1030), 3))
    statemachineOutput.size mustBe 2
    statemachineOutput(1) mustBe SendDownstream(RaftClusterClient(DOWNSTREAM_CLUSTER, registeredId), Write(10, 100))
    eventSubject.onNext(CommitLogEntry(RemoveCommandsEntry(1, registeredDownstreamClient, 0, 1040), 4))
    statemachineOutput.size mustBe 3
    statemachineOutput(2) mustBe RemovedCommands(registeredDownstreamClient, 0)
    eventSubject.onNext(CommitLogEntry(ExpireSessionsEntry(1, 1200), 5))
    statemachineOutput.size mustBe 4
    statemachineOutput(3) mustBe DownstreamClientExpired(registeredDownstreamClient)
  }

  @Test
  def removesCorrectExpiredClientOnDeliveryOfLastCommand(): Unit = {
    val registeredId = "1"
    val eventSubject = PublishSubject[CommitLogEntry]()
    val registeredDownstreamClient = RaftClusterClient(DOWNSTREAM_CLUSTER, registeredId)
    var statemachineOutput = Seq[StatemachineOutputEvent]()
    val statemachineModule = StatemachineModule(ADDRESS, TestStatemachine(100), eventSubject, ImmediateScheduler())
    statemachineModule.observable.subscribe(event => statemachineOutput = statemachineOutput :+ event, e => throw e)
    eventSubject.onNext(registerClientEntry)
    eventSubject.onNext(commitEntryEvent(0, DownstreamWrite(10, 10, DOWNSTREAM_CLUSTER), 1020))
    statemachineOutput.size mustBe 1
    statemachineOutput.head mustBe RegisterDownstreamClient(DOWNSTREAM_PRODUCER)
    eventSubject.onNext(CommitLogEntry(RegisteredDownstreamClientEntry(1, DOWNSTREAM_PRODUCER, registeredId, 1030), 3))
    statemachineOutput.size mustBe 2
    statemachineOutput(1) mustBe SendDownstream(RaftClusterClient(DOWNSTREAM_CLUSTER, registeredId), Write(10, 100))
    eventSubject.onNext(CommitLogEntry(ExpireSessionsEntry(1, 1200), 4))
    statemachineOutput.size mustBe 2
    eventSubject.onNext(CommitLogEntry(RemoveCommandsEntry(1, registeredDownstreamClient, 0, 1240), 5))
    statemachineOutput.size mustBe 4
    statemachineOutput(2) mustBe RemovedCommands(registeredDownstreamClient, 0)
    statemachineOutput(3) mustBe DownstreamClientExpired(registeredDownstreamClient)
  }

  private def clientCommandEvent(seqNum: Int, command: StatemachineCommand, timestamp: Long) =
    ClientCommandEvent(CLIENT_ID, seqNum, seqNum, timestamp, ByteStringSerializer.write(command))(Observer())

  private def commitEntryEvent(seqNum: Int, command: StatemachineCommand, timestamp: Long) =
    CommitLogEntry(clientCommandEvent(seqNum, command, timestamp).toLogEntry(PersistentState(1, None, RaftLog(CLUSTER_CONFIG))), 2)


}
