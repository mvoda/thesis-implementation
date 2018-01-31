package rx.distributed.raft.rpc.server

import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.logging.{Level, Logger}

import io.grpc.inprocess.InProcessChannelBuilder
import org.apache.logging.log4j.scala.Logging
import org.junit.Test
import org.scalatest.MustMatchers._
import rx.distributed.raft.LatchObservers.OnNextCountdown
import rx.distributed.raft._
import rx.distributed.raft.consensus.server.Address.TestingAddress
import rx.distributed.raft.converters.ObserverConverters
import rx.distributed.raft.events.Event
import rx.distributed.raft.events.client.{ClientCommandEvent, ClientEvent, KeepAliveEvent, RegisterClientEvent}
import rx.distributed.raft.events.rpc.request.{AppendEntriesRpcRequest, ConsensusRpcRequest, VoteRequestRpcRequest}
import rx.lang.scala.Subscriber


class RaftRPCServerTest extends Logging {
  def createClientFutureStub(name: String): RaftClientGrpc.RaftClientFutureStub = {
    RaftClientGrpc.newFutureStub(InProcessChannelBuilder.forName(name).build())
  }

  def createStub(name: String): RaftClientGrpc.RaftClientStub = {
    RaftClientGrpc.newStub(InProcessChannelBuilder.forName(name).build())
  }

  def createConsensusFutureStub(name: String): RaftGrpc.RaftFutureStub = {
    RaftGrpc.newFutureStub(InProcessChannelBuilder.forName(name).build())
  }

  @Test
  def sendsClientEvents(): Unit = {
    val eventObserver = OnNextCountdown[Event](3)
    val serverAddress = randomTestAddress()
    val subscription = RaftRPCServer(serverAddress).observable.subscribe(eventObserver.observer)
    sendClientEvents(serverAddress)
    eventObserver.latch.await(3, TimeUnit.SECONDS)
    eventObserver.events.size mustBe 3
    // can't assume ordering of events
    eventObserver.events.count(_.isInstanceOf[RegisterClientEvent]) mustBe 1
    eventObserver.events.count(_.isInstanceOf[KeepAliveEvent]) mustBe 1
    eventObserver.events.count(_.isInstanceOf[ClientCommandEvent]) mustBe 1
    subscription.unsubscribe()
  }

  @Test
  def sendsConsensusEvents(): Unit = {
    val eventObserver = OnNextCountdown[Event](2)
    val serverAddress = randomTestAddress()
    val subscription = RaftRPCServer(serverAddress).observable.subscribe(eventObserver.observer)
    sendConsensusEvents(serverAddress)
    eventObserver.latch.await(3, TimeUnit.SECONDS)
    eventObserver.events.size mustBe 2
    // can't assume ordering of events
    eventObserver.events.count(_.isInstanceOf[AppendEntriesRpcRequest]) mustBe 1
    eventObserver.events.count(_.isInstanceOf[VoteRequestRpcRequest]) mustBe 1
    subscription.unsubscribe()
  }

  @Test
  def sendsMixedEvents(): Unit = {
    val eventObserver = OnNextCountdown[Event](5)
    val serverAddress = randomTestAddress()
    val subscription = RaftRPCServer(serverAddress).observable.subscribe(eventObserver.observer)
    sendConsensusEvents(serverAddress)
    sendClientEvents(serverAddress)
    eventObserver.latch.await(3, TimeUnit.SECONDS)
    eventObserver.events.size mustBe 5
    // can't assume ordering of events
    eventObserver.events.count(_.isInstanceOf[ConsensusRpcRequest]) mustBe 2
    eventObserver.events.count(_.isInstanceOf[ClientEvent]) mustBe 3
    subscription.unsubscribe()
  }

  private def sendClientEvents(address: TestingAddress): Unit = {
    Logger.getLogger("io.grpc").setLevel(Level.OFF)
    val futureStub = createClientFutureStub(address.name)
    futureStub.registerClient(RegisterClientRequestProto.newBuilder().build())
    futureStub.keepAlive(KeepAliveRequestProto.newBuilder().build())
    val stub = createStub(address.name)
    val commandObserver = stub.clientRequest(ObserverConverters.fromRx(Subscriber()))
    commandObserver.onNext(ClientRequestProto.newBuilder().build())
  }

  private def sendConsensusEvents(address: TestingAddress): Unit = {
    Logger.getLogger("io.grpc").setLevel(Level.OFF)
    val stub = createConsensusFutureStub(address.name)
    stub.appendEntries(AppendEntriesRequestProto.newBuilder().build())
    stub.requestVote(VoteRequestProto.newBuilder().build())
  }

  private def randomTestAddress(): TestingAddress = TestingAddress(UUID.randomUUID().toString)
}
