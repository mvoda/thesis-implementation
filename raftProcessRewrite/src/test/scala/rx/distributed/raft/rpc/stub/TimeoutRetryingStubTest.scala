package rx.distributed.raft.rpc.stub

import com.google.protobuf.ByteString
import io.grpc.Status
import org.apache.logging.log4j.scala.Logging
import org.junit.{After, Test}
import org.scalatest.MustMatchers._
import rx.distributed.raft.LatchObservers.{OnCompletedCountdown, OnNextCountdown}
import rx.distributed.raft.consensus.server.Address.TestingAddress
import rx.distributed.raft.events.client.{ClientCommandEvent, KeepAliveEvent, RegisterClientEvent}
import rx.distributed.raft.events.rpc.response._
import rx.distributed.raft.timeout.RxTimeout
import rx.distributed.raft.util.{CommandQueue, IndexedCommand}
import rx.distributed.raft.{KeepAliveRequestProto, Utils}
import rx.lang.scala.subjects.ReplaySubject
import rx.lang.scala.{Observable, Subscription}

import scala.concurrent.duration.{Duration, MINUTES, SECONDS}
import scala.util.Random

class TimeoutRetryingStubTest extends Logging {
  private val ADDRESS = TestingAddress("test_server_1")
  private val CLIENT_ID = "client"
  private val clientCommands = Observable.just(IndexedCommand(1, null), IndexedCommand(2, null), IndexedCommand(3, null))

  private var clientSubscription: Option[Subscription] = None
  private var serverSubscription: Option[Subscription] = None

  @Test
  def givesUpAfterTimeout(): Unit = {
    val latchObserver = OnNextCountdown[String](1)
    val proxy = TimeoutRetryingStub(Set(ADDRESS), 5)
    clientSubscription = Some(proxy.registerClient(Seq()).subscribe(latchObserver.observer))
    latchObserver.latch.await()
    latchObserver.events.size mustBe 0
    latchObserver.errors.size mustBe 1
  }

  @Test
  def stopsTimeoutAfterResponse(): Unit = {
    val clientId = "new_client_1"
    val clientObserver = OnCompletedCountdown[String](1)
    serverSubscription = Some(Utils.createCommandServer(ADDRESS.name).delay(Duration(3, SECONDS)).subscribe(request =>
      Observable.just(RegisterClientSuccessful(clientId)).subscribe(request.asInstanceOf[RegisterClientEvent].observer)
    ))
    val proxy = TimeoutRetryingStub(Set(ADDRESS), 5)
    clientSubscription = Some(proxy.registerClient(Seq()).subscribe(clientObserver.observer))

    clientObserver.latch.await()
    clientObserver.events.size mustBe 1
    clientObserver.events.head mustBe clientId
    clientObserver.errors.size mustBe 0
  }

  @Test
  def doesNotTimeoutOnReceivingNotLeader(): Unit = {
    var count = 0
    val clientId = "new_client_1"
    val clientObserver = OnNextCountdown[String](1)
    serverSubscription = Some(Utils.createCommandServer(ADDRESS.name).delay(Duration(3, SECONDS)).subscribe(request => {
      val responseObserver = request.asInstanceOf[RegisterClientEvent].observer
      if (count < 3) {
        Observable.just(RegisterClientNotLeader(Some(ADDRESS))).subscribe(responseObserver)
        count = count + 1
      }
      else Observable.just(RegisterClientSuccessful(clientId)).subscribe(responseObserver)
    }))
    val proxy = TimeoutRetryingStub(Set(ADDRESS), 5)
    clientSubscription = Some(proxy.registerClient(Seq()).subscribe(clientObserver.observer))

    clientObserver.latch.await()
    clientObserver.events.size mustBe 1
    clientObserver.events.head mustBe clientId
    clientObserver.errors.size mustBe 0
  }

  @Test
  def timesOutWhenServerRespondsWithError(): Unit = {
    val clientObserver = OnNextCountdown[String](1)
    serverSubscription = Some(Utils.createCommandServer(ADDRESS.name).delay(Duration(3, SECONDS)).subscribe(request => {
      val responseObserver = request.asInstanceOf[RegisterClientEvent].observer
      Observable.error(Status.INTERNAL.asException()).subscribe(responseObserver)
    }))
    val proxy = TimeoutRetryingStub(Set(ADDRESS), 5)
    clientSubscription = Some(proxy.registerClient(Seq()).subscribe(clientObserver.observer))

    clientObserver.latch.await()
    clientObserver.events.size mustBe 0
    clientObserver.errors.size mustBe 1
  }

  @Test
  def timesOutWhenServerRespondsWithErrorAfter3NotLeader(): Unit = {
    var count = 0
    val clientObserver = OnNextCountdown[String](1)
    serverSubscription = Some(Utils.createCommandServer(ADDRESS.name).delay(Duration(3, SECONDS)).subscribe(request => {
      val responseObserver = request.asInstanceOf[RegisterClientEvent].observer
      if (count < 3) {
        Observable.just(RegisterClientNotLeader(Some(ADDRESS))).subscribe(responseObserver)
        count = count + 1
      } else {
        Observable.error(Status.INTERNAL.asException()).subscribe(responseObserver)
      }
    }))
    val proxy = TimeoutRetryingStub(Set(ADDRESS), 5)
    clientSubscription = Some(proxy.registerClient(Seq()).subscribe(clientObserver.observer))

    clientObserver.latch.await()
    clientObserver.events.size mustBe 0
    clientObserver.errors.size mustBe 1
  }

  @Test
  def keepsRetryingAfterErrors(): Unit = {
    var count = 0
    val clientId = "new_client_1"
    val clientObserver = OnNextCountdown[String](1)
    serverSubscription = Some(Utils.createCommandServer(ADDRESS.name).delay(Duration(1, SECONDS)).subscribe(request => {
      val responseObserver = request.asInstanceOf[RegisterClientEvent].observer
      if (count < 3) {
        Observable.error(Status.INTERNAL.asException()).subscribe(responseObserver)
        count = count + 1
      }
      else Observable.just(RegisterClientSuccessful(clientId)).subscribe(responseObserver)
    }))
    val proxy = TimeoutRetryingStub(Set(ADDRESS), 10)
    clientSubscription = Some(proxy.registerClient(Seq()).subscribe(clientObserver.observer))

    clientObserver.latch.await()
    clientObserver.events.size mustBe 1
    clientObserver.events.head mustBe clientId
    clientObserver.errors.size mustBe 0
  }

  @Test
  def timesOutWhenServerTakesTooLongToRespond(): Unit = {
    var count = 0
    val clientObserver = OnNextCountdown[String](1)
    serverSubscription = Some(Utils.createCommandServer(ADDRESS.name).delay(Duration(3, SECONDS)).subscribe(request => {
      val responseObserver = request.asInstanceOf[RegisterClientEvent].observer
      if (count < 3) {
        Observable.just(RegisterClientNotLeader(Some(ADDRESS))).subscribe(responseObserver)
        count = count + 1
      } else Observable.just(RegisterClientNotLeader(Some(ADDRESS))).delay(Duration(1, MINUTES)).subscribe(responseObserver)
    }))
    val proxy = TimeoutRetryingStub(Set(ADDRESS), 5)
    clientSubscription = Some(proxy.registerClient(Seq()).subscribe(clientObserver.observer))

    clientObserver.latch.await()
    clientObserver.events.size mustBe 0
    clientObserver.errors.size mustBe 1
  }

  @Test
  def stopsTimeoutAfterKeepAliveResponses(): Unit = {
    val clientObserver = OnCompletedCountdown[KeepAliveResponse](1)
    serverSubscription = Some(Utils.createCommandServer(ADDRESS.name).delay(Duration(1, SECONDS)).subscribe(request =>
      Observable.just(KeepAliveSuccessful()).subscribe(request.asInstanceOf[KeepAliveEvent].observer)
    ))
    val proxy = TimeoutRetryingStub(Set(ADDRESS), 5)
    clientSubscription = Some(proxy.keepAlive(Observable.just(createKeepAliveReq())).subscribe(clientObserver.observer))

    clientObserver.latch.await()
    clientObserver.events.size mustBe 1
    clientObserver.events.count(_.isInstanceOf[KeepAliveSuccessful]) mustBe 1
    clientObserver.errors.size mustBe 0
  }

  @Test
  def keepsAliveDespiteNoClientRequests(): Unit = {
    val clientObserver = OnCompletedCountdown[ClientCommandResponse](1)
    serverSubscription = Some(Utils.createCommandServer(ADDRESS.name).delay(Duration(1, SECONDS)).subscribe(request =>
      request match {
        case keepAlive: KeepAliveEvent => Observable.just(KeepAliveSuccessful()).subscribe(keepAlive.observer)
        case clientEvent: ClientCommandEvent =>
          Observable.just(ClientResponseSuccessful(clientEvent.seqNum, ByteString.EMPTY)).subscribe(clientEvent.observer)
      }
    ))
    val keepAliveTimeout = RxTimeout(1000, 1000)
    val keepAliveSource = keepAliveTimeout.map(_ => createKeepAliveReq())
    val commandSource = clientCommands.delay(Duration(7, SECONDS)).map(_.toProtobuf("client", 1))
    val proxy = TimeoutRetryingStub(Set(ADDRESS), 5)
    keepAliveTimeout.start()
    clientSubscription = Some(proxy.clientCommands(commandSource, keepAliveSource).subscribe(clientObserver.observer))

    clientObserver.latch.await()
    clientObserver.events.size mustBe 3
    val responses = clientObserver.events.map(_.asInstanceOf[ClientResponseSuccessful])
    responses.size mustBe 3
    responses.head.seqNum mustBe 1
    responses(1).seqNum mustBe 2
    responses(2).seqNum mustBe 3
    clientObserver.errors.size mustBe 0
  }

  @Test
  def stopsTimeoutAfterClientCommandResponses(): Unit = {
    val clientObserver = OnCompletedCountdown[ClientCommandResponse](1)
    serverSubscription = Some(Utils.createCommandServer(ADDRESS.name).delay(Duration(1, SECONDS)).subscribe(request =>
      Observable.just(ClientResponseSuccessful(request.asInstanceOf[ClientCommandEvent].seqNum, ByteString.EMPTY))
        .subscribe(request.asInstanceOf[ClientCommandEvent].observer)
    ))
    val proxy = TimeoutRetryingStub(Set(ADDRESS), 5)
    val commandSubject = ReplaySubject[IndexedCommand]()
    clientCommands.subscribe(commandSubject)
    clientSubscription = Some(proxy.clientCommands(commandSubject.map(_.toProtobuf("client", 1))).subscribe(clientObserver.observer))

    clientObserver.latch.await()
    clientObserver.events.size mustBe 3
    val responses = clientObserver.events.map(_.asInstanceOf[ClientResponseSuccessful])
    responses.size mustBe 3
    responses.head.seqNum mustBe 1
    responses(1).seqNum mustBe 2
    responses(2).seqNum mustBe 3
    clientObserver.errors.size mustBe 0
  }

  @Test
  def respondsToAllRequestsInCaseOfFailure(): Unit = {
    var count = 0
    val clientObserver = OnCompletedCountdown[ClientCommandResponse](1)
    serverSubscription = Some(Utils.createCommandServer(ADDRESS.name).delay(Duration(1, SECONDS)).subscribe(request => {
      if (count < 2) {
        Observable.error(Status.INTERNAL.asException()).subscribe(request.asInstanceOf[ClientCommandEvent].observer)
      } else {
        Observable.just(ClientResponseSuccessful(request.asInstanceOf[ClientCommandEvent].seqNum, ByteString.EMPTY))
          .subscribe(request.asInstanceOf[ClientCommandEvent].observer)
      }
      count = count + 1
    }))
    val proxy = TimeoutRetryingStub(Set(ADDRESS), 5)
    val commandSubject = ReplaySubject[IndexedCommand]()
    clientCommands.subscribe(commandSubject)
    clientSubscription = Some(proxy.clientCommands(commandSubject.map(_.toProtobuf("client", 1))).subscribe(clientObserver.observer))

    clientObserver.latch.await()
    clientObserver.events.size mustBe 3
    val responses = clientObserver.events.map(_.asInstanceOf[ClientResponseSuccessful])
    responses.size mustBe 3
    responses.head.seqNum mustBe 1
    responses(1).seqNum mustBe 2
    responses(2).seqNum mustBe 3
    clientObserver.errors.size mustBe 0
  }

  @Test
  def respondsToRequestsInCaseOfRandomFailure(): Unit = {
    val commandQueue = CommandQueue(clientCommands)
    val clientObserver = OnCompletedCountdown[ClientCommandResponse](1, response =>
      commandQueue.addResponseSeqNum(response.asInstanceOf[ClientResponseSuccessful].seqNum))
    serverSubscription = Some(Utils.createCommandServer(ADDRESS.name).subscribe(request => {
      if (Random.nextInt() % 2 == 0) {
        Observable.error(Status.INTERNAL.asException()).subscribe(request.asInstanceOf[ClientCommandEvent].observer)
      } else {
        Observable.just(ClientResponseSuccessful(request.asInstanceOf[ClientCommandEvent].seqNum, ByteString.EMPTY))
          .subscribe(request.asInstanceOf[ClientCommandEvent].observer)
      }
    }))
    val proxy = TimeoutRetryingStub(Set(ADDRESS), 5)
    clientSubscription = Some(proxy.clientCommands(commandQueue.commands.map(_.toProtobuf("client", 1))).subscribe(clientObserver.observer))

    clientObserver.latch.await()
    clientObserver.events.size mustBe 3
    val responses = clientObserver.events.map(_.asInstanceOf[ClientResponseSuccessful])
    responses.size mustBe 3
    responses.head.seqNum mustBe 1
    responses(1).seqNum mustBe 2
    responses(2).seqNum mustBe 3
    clientObserver.errors.size mustBe 0
  }

  @After
  def afterEach(): Unit = {
    clientSubscription.foreach(_.unsubscribe())
    serverSubscription.foreach(_.unsubscribe())
  }

  private def createKeepAliveReq(responseSeqNum: Int = 0) =
    KeepAliveRequestProto.newBuilder().setClientId(CLIENT_ID).setResponseSequenceNum(responseSeqNum).build()
}
