package rx.distributed.raft.rpc.stub

import java.util.concurrent.TimeUnit

import io.grpc.Status
import org.apache.logging.log4j.scala.Logging
import org.junit.{After, Test}
import org.scalatest.MustMatchers._
import rx.distributed.raft.LatchObservers.{LatchObserver, OnCompletedCountdown, OnNextCountdown}
import rx.distributed.raft.Utils
import rx.distributed.raft.consensus.server.Address.TestingAddress
import rx.distributed.raft.events.client.RegisterClientEvent
import rx.distributed.raft.events.rpc.response.{RegisterClientNotLeader, RegisterClientSuccessful, RpcResponse}
import rx.distributed.raft.rpc.proxy.ClusterStub
import rx.lang.scala.{Observable, Subscription}

import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}

class RetryingStubTest extends Logging {
  private val ADDRESS = TestingAddress("test_server_1")
  private var clientSubscription: Option[Subscription] = None
  private var serverSubscription: Option[Subscription] = None

  @Test
  def retriesWhenUnableToReachServer(): Unit = {
    val latchObserver = OnNextCountdown[RpcResponse](1)
    val stub = RetryingStub(Set(ADDRESS))
    clientSubscription = Some(stub.leaderRequest((stub) => ClusterStub.registerClient(stub, Seq())).subscribe(latchObserver.observer))
    latchObserver.latch.await(3, TimeUnit.SECONDS)
    latchObserver.events.size mustBe 0
    latchObserver.errors.size mustBe 0
  }

  @Test
  def stopsRetryingAfterResponse(): Unit = {
    val clientId = "new_client_1"
    val clientObserver = OnCompletedCountdown[RpcResponse](1)
    serverSubscription = Some(Utils.createCommandServer(ADDRESS.name).delay(Duration(3, SECONDS)).subscribe(request =>
      Observable.just(RegisterClientSuccessful(clientId)).subscribe(request.asInstanceOf[RegisterClientEvent].observer)
    ))
    val stub = RetryingStub(Set(ADDRESS))
    clientSubscription = Some(stub.leaderRequest((stub) => ClusterStub.registerClient(stub, Seq())).subscribe(clientObserver.observer))

    getSingleLatchEvent(clientObserver) mustBe RegisterClientSuccessful(clientId)
  }

  @Test
  def retriesWhenReceivesNotLeader(): Unit = {
    val clientObserver = OnCompletedCountdown[RpcResponse](1)
    serverSubscription = Some(Utils.createCommandServer(ADDRESS.name).delay(Duration(500, MILLISECONDS)).subscribe(request =>
      Observable.just(RegisterClientNotLeader(None)).subscribe(request.asInstanceOf[RegisterClientEvent].observer)
    ))
    val stub = RetryingStub(Set(ADDRESS))
    clientSubscription = Some(stub.leaderRequest((stub) => ClusterStub.registerClient(stub, Seq())).subscribe(clientObserver.observer))

    clientObserver.latch.await(3, TimeUnit.SECONDS)
    clientObserver.events.size mustBe 0
    clientObserver.errors.size mustBe 0
  }

  @Test
  def retriesOnErrorResponse(): Unit = {
    val clientObserver = OnCompletedCountdown[RpcResponse](1)
    serverSubscription = Some(Utils.createCommandServer(ADDRESS.name).delay(Duration(1, SECONDS)).subscribe(request =>
      Observable.error(Status.INTERNAL.asException()).subscribe(request.asInstanceOf[RegisterClientEvent].observer)
    ))
    val stub = RetryingStub(Set(ADDRESS))
    clientSubscription = Some(stub.leaderRequest((stub) => ClusterStub.registerClient(stub, Seq())).subscribe(clientObserver.observer))

    clientObserver.latch.await(5, TimeUnit.SECONDS)
    clientObserver.events.size mustBe 0
    clientObserver.errors.size mustBe 0
  }

  @Test
  def returnsCorrectResponseAfterFewNotLeader(): Unit = {
    var count = 0
    val clientId = "new_client_1"
    val clientObserver = OnCompletedCountdown[RpcResponse](1)
    serverSubscription = Some(Utils.createCommandServer(ADDRESS.name).delay(Duration(1, SECONDS)).subscribe(request => {
      val responseObserver = request.asInstanceOf[RegisterClientEvent].observer
      if (count < 5) {
        Observable.just(RegisterClientNotLeader(None)).subscribe(responseObserver)
        count = count + 1
      }
      else Observable.just(RegisterClientSuccessful(clientId)).subscribe(responseObserver)
    }))
    val stub = RetryingStub(Set(ADDRESS))
    clientSubscription = Some(stub.leaderRequest((stub) => ClusterStub.registerClient(stub, Seq())).subscribe(clientObserver.observer))

    getSingleLatchEvent(clientObserver) mustBe RegisterClientSuccessful(clientId)
  }

  @Test
  def returnsCorrectResponseAfterFewErrors(): Unit = {
    var count = 0
    val clientId = "new_client_1"
    val clientObserver = OnCompletedCountdown[RpcResponse](1)
    serverSubscription = Some(Utils.createCommandServer(ADDRESS.name).delay(Duration(1, SECONDS)).subscribe(request => {
      val responseObserver = request.asInstanceOf[RegisterClientEvent].observer
      if (count < 5) {
        Observable.error(Status.INTERNAL.asException()).subscribe(responseObserver)
      }
      else Observable.just(RegisterClientSuccessful(clientId)).subscribe(responseObserver)
      count = count + 1
    }))
    val stub = RetryingStub(Set(ADDRESS))
    clientSubscription = Some(stub.leaderRequest((stub) => ClusterStub.registerClient(stub, Seq())).subscribe(clientObserver.observer))

    getSingleLatchEvent(clientObserver) mustBe RegisterClientSuccessful(clientId)
  }


  @Test
  def returnsFirstResponse(): Unit = {
    var count = 0
    val clientId = "new_client_"
    val clientObserver = OnCompletedCountdown[RpcResponse](1)
    serverSubscription = Some(Utils.createCommandServer(ADDRESS.name).subscribe(request => {
      val responseObserver = request.asInstanceOf[RegisterClientEvent].observer
      count = count + 1
      Observable.just(RegisterClientSuccessful(clientId + count)).subscribe(responseObserver)
    }))
    val stub = RetryingStub(Set(ADDRESS))
    clientSubscription = Some(stub.leaderRequest((stub) => ClusterStub.registerClient(stub, Seq())).subscribe(clientObserver.observer))

    getSingleLatchEvent(clientObserver) mustBe RegisterClientSuccessful(clientId + 1)
  }

  private def getSingleLatchEvent[T](latchObserver: LatchObserver[T]): T = {
    latchObserver.latch.await()
    latchObserver.errors.size mustBe 0
    latchObserver.events.size mustBe 1
    latchObserver.events.head
  }

  @After
  def afterEach(): Unit = {
    clientSubscription.foreach(_.unsubscribe())
    serverSubscription.foreach(_.unsubscribe())
  }
}
