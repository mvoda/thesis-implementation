package rx.distributed.raft.rpc.server

import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.logging.{Level, Logger}

import io.grpc.inprocess.InProcessChannelBuilder
import org.junit.Test
import org.scalatest.MustMatchers._
import rx.distributed.raft.LatchObservers.OnNextCountdown
import rx.distributed.raft._
import rx.distributed.raft.converters.ObserverConverters
import rx.distributed.raft.events.client.{ClientCommandEvent, KeepAliveEvent, RegisterClientEvent}
import rx.distributed.raft.events.rpc.request.ClientRpcRequest
import rx.lang.scala.Subscriber

class CommandRPCServerTest {
  Logger.getLogger("io.grpc").setLevel(Level.OFF)

  def createFutureStub(name: String): RaftClientGrpc.RaftClientFutureStub = {
    RaftClientGrpc.newFutureStub(InProcessChannelBuilder.forName(name).build())
  }

  def createStub(name: String): RaftClientGrpc.RaftClientStub = {
    RaftClientGrpc.newStub(InProcessChannelBuilder.forName(name).build())
  }

  @Test
  def sendsRegisterClient(): Unit = {
    val eventObserver = OnNextCountdown[ClientRpcRequest](1)
    val serverName = UUID.randomUUID().toString
    val stub = createFutureStub(serverName)
    val subscription = Utils.createCommandServer(serverName).subscribe(eventObserver.observer)
    stub.registerClient(RegisterClientRequestProto.newBuilder().build())
    eventObserver.latch.await(3, TimeUnit.SECONDS)
    eventObserver.events.size mustBe 1
    eventObserver.events.head mustBe an[RegisterClientEvent]
    subscription.unsubscribe()
  }

  @Test
  def sendsKeepAlive(): Unit = {
    val eventObserver = OnNextCountdown[ClientRpcRequest](1)
    val serverName = UUID.randomUUID().toString
    val stub = createFutureStub(serverName)
    val subscription = Utils.createCommandServer(serverName).subscribe(eventObserver.observer)
    stub.keepAlive(KeepAliveRequestProto.newBuilder().build())
    eventObserver.latch.await(3, TimeUnit.SECONDS)
    eventObserver.events.size mustBe 1
    eventObserver.events.head mustBe an[KeepAliveEvent]
    subscription.unsubscribe()
  }

  @Test
  def sendsClientCommand(): Unit = {
    val eventObserver = OnNextCountdown[ClientRpcRequest](3)
    val serverName = UUID.randomUUID().toString
    val stub = createStub(serverName)
    val subscription = Utils.createCommandServer(serverName).subscribe(eventObserver.observer)
    val commandObserver = stub.clientRequest(ObserverConverters.fromRx(Subscriber()))
    commandObserver.onNext(ClientRequestProto.newBuilder().build())
    commandObserver.onNext(ClientRequestProto.newBuilder().build())
    commandObserver.onNext(ClientRequestProto.newBuilder().build())
    eventObserver.latch.await(3, TimeUnit.SECONDS)
    eventObserver.events.size mustBe 3
    eventObserver.events.head mustBe an[ClientCommandEvent]
    eventObserver.events(1) mustBe an[ClientCommandEvent]
    eventObserver.events(2) mustBe an[ClientCommandEvent]
    subscription.unsubscribe()
  }
}
