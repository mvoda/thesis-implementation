package rx.distributed.raft.rpc.client

import java.util.UUID

import io.grpc.Status
import org.apache.logging.log4j.scala.Logging
import org.junit.{After, Test}
import org.scalatest.MustMatchers._
import rx.distributed.raft.LatchObservers.OnCompletedCountdown
import rx.distributed.raft.Utils
import rx.distributed.raft.consensus.server.Address.TestingAddress
import rx.distributed.raft.events.client.{ClientCommandEvent, KeepAliveEvent, RegisterClientEvent}
import rx.distributed.raft.events.rpc.response.{ClientResponseSuccessful, KeepAliveSuccessful, RegisterClientSuccessful}
import rx.distributed.raft.statemachine.input.StatemachineCommand
import rx.distributed.raft.statemachine.output.StatemachineResponse
import rx.distributed.raft.util.{ByteStringDeserializer, ByteStringSerializer}
import rx.lang.scala.{Observable, Subscription}

import scala.concurrent.duration.{Duration, SECONDS}
import scala.util.Random

class RaftClientTest extends Logging {
  private val ADDRESS = TestingAddress("test_server_1")
  private val statemachineCommands = Seq(TestCommand(3), TestCommand(2), TestCommand(1))
  private val commandsObservable = Observable.from(statemachineCommands)
  private val expectedResponses = Seq(TestResponse(30), TestResponse(20), TestResponse(10))
  private var clientSubscription: Option[Subscription] = None
  private var serverSubscription: Option[Subscription] = None

  @Test
  def keepsAliveAndReceivesCorrectResponses(): Unit = {
    val clientObserver = OnCompletedCountdown[StatemachineResponse](1)
    serverSubscription = Some(Utils.createCommandServer(ADDRESS.name).delay(Duration(1, SECONDS)).subscribe(request =>
      request match {
        case registerClient: RegisterClientEvent => Observable.just(RegisterClientSuccessful(createClientId())).subscribe(registerClient.observer)
        case keepAlive: KeepAliveEvent => Observable.just(KeepAliveSuccessful()).subscribe(keepAlive.observer)
        case clientEvent: ClientCommandEvent =>
          val serializedResponse = ByteStringSerializer.write(response(clientEvent))
          Observable.just(ClientResponseSuccessful(clientEvent.seqNum, serializedResponse)).subscribe(clientEvent.observer)
      }
    ))

    val raftClient = RaftClient(Set(ADDRESS), 5, 1, Seq())
    clientSubscription = Some(raftClient.sendCommands(commandsObservable.delay(Duration(7, SECONDS))).subscribe(clientObserver.observer))

    clientObserver.latch.await()
    clientObserver.events.size mustBe 3
    clientObserver.events mustBe expectedResponses
    clientObserver.errors.size mustBe 0
  }

  @Test
  def receivesCorrectResponsesWithRandomErrors(): Unit = {
    var consecutiveErrors = 0
    val clientObserver = OnCompletedCountdown[StatemachineResponse](1)
    serverSubscription = Some(Utils.createCommandServer(ADDRESS.name).subscribe(request => {
      request match {
        case registerClient: RegisterClientEvent => Observable.just(RegisterClientSuccessful(createClientId())).subscribe(registerClient.observer)
        case keepAlive: KeepAliveEvent =>
          if (Random.nextInt() % 2 == 0 && consecutiveErrors < 2) {
            consecutiveErrors += 1
            Observable.error(Status.INTERNAL.asException()).subscribe(keepAlive.observer)
          } else {
            consecutiveErrors = 0
            Observable.just(KeepAliveSuccessful()).subscribe(keepAlive.observer)
          }
        case clientEvent: ClientCommandEvent =>
          if (Random.nextInt() % 2 == 0) {
            val serializedResponse = ByteStringSerializer.write(response(clientEvent))
            Observable.just(ClientResponseSuccessful(clientEvent.seqNum, serializedResponse)).subscribe(clientEvent.observer)
          } else {
            Observable.error(Status.INTERNAL.asException()).subscribe(clientEvent.observer)
          }
      }
    }))

    val raftClient = RaftClient(Set(ADDRESS), 5, 1, Seq())
    clientSubscription = Some(raftClient.sendCommands(commandsObservable.delay(Duration(7, SECONDS))).subscribe(clientObserver.observer))

    clientObserver.latch.await()
    clientObserver.events.size mustBe 3
    clientObserver.events mustBe expectedResponses
    clientObserver.errors.size mustBe 0
  }

  @Test
  def correctlyUpdatesResponseSeqNum(): Unit = {
    val clientObserver = OnCompletedCountdown[StatemachineResponse](1)
    serverSubscription = Some(Utils.createCommandServer(ADDRESS.name).subscribe(request => {
      request match {
        case registerClient: RegisterClientEvent => Observable.just(RegisterClientSuccessful(createClientId())).subscribe(registerClient.observer)
        case keepAlive: KeepAliveEvent => Observable.just(KeepAliveSuccessful()).subscribe(keepAlive.observer)
        case clientEvent: ClientCommandEvent =>
          val serializedResponse = ByteStringSerializer.write(TestResponse(clientEvent.responseSeqNum * 10))
          Observable.just(ClientResponseSuccessful(clientEvent.seqNum, serializedResponse))
            .subscribe(clientEvent.observer)
      }
    }))

    val raftClient = RaftClient(Set(ADDRESS), 5, 1, Seq())
    val delayedCommands = Observable.interval(Duration(2, SECONDS)).take(3).zipWithIndex.map(pair => statemachineCommands(pair._2))
    clientSubscription = Some(raftClient.sendCommands(delayedCommands).subscribe(clientObserver.observer))
    clientObserver.latch.await()
    clientObserver.events.size mustBe 3
    clientObserver.events mustBe Seq(TestResponse(0), TestResponse(10), TestResponse(20))
    clientObserver.errors.size mustBe 0
  }

  @After
  def afterEach(): Unit = {
    clientSubscription.foreach(_.unsubscribe())
    serverSubscription.foreach(_.unsubscribe())
  }

  private def createClientId(): String = {
    UUID.randomUUID().toString
  }

  private def response(clientEvent: ClientCommandEvent): StatemachineResponse = {
    val command = ByteStringDeserializer.deserialize[StatemachineCommand](clientEvent.command)
    command match {
      case TestCommand(testValue) => TestResponse(testValue * 10)
      case _ => UnknownCommand()
    }
  }
}

case class TestCommand(testValue: Int) extends StatemachineCommand

case class TestResponse(testValue: Int) extends StatemachineResponse

case class UnknownCommand() extends StatemachineResponse
