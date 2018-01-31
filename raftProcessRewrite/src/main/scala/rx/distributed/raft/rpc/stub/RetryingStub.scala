package rx.distributed.raft.rpc.stub

import java.util.logging.{Level, Logger}

import io.grpc.ManagedChannel
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.netty.NettyChannelBuilder
import org.apache.logging.log4j.scala.Logging
import rx.distributed.raft.RaftClientGrpc
import rx.distributed.raft.RaftClientGrpc.RaftClientStub
import rx.distributed.raft.consensus.server.Address.{RemoteAddress, ServerAddress, TestingAddress}
import rx.distributed.raft.events.rpc.response.{NotLeaderRpcResponse, RpcResponse}
import rx.distributed.raft.events.rpc.stub.{CreateNewStub, StubEvent, SwitchChannel}
import rx.distributed.raft.util.RaftStubUtils
import rx.lang.scala.Observable
import rx.lang.scala.schedulers.ImmediateScheduler
import rx.lang.scala.subjects.{BehaviorSubject, PublishSubject}

import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.util.Random

case class RetryingStub(cluster: Set[ServerAddress]) extends Logging {
  Logger.getLogger("io.grpc").setLevel(Level.OFF)
  private val stubSubject = BehaviorSubject[RaftClientStub]()
  private val switchChannelEvents = PublishSubject[StubEvent]()

  switchChannelEvents.distinctUntilChanged.flatMap {
    case CreateNewStub() => createStub()
    case event: SwitchChannel => switchChannel(event)
  }.subscribe(stub => stubSubject.onNext(stub))

  val leaderStub: Observable[RaftClientStub] = stubSubject

  switchChannelEvents.onNext(CreateNewStub())

  //TODO: could this produce 2 client ids?
  def leaderRequest[T <: RpcResponse](function: (RaftClientStub) => Observable[T]): Observable[T] = {
    leaderStub.take(1).flatMap(stub => {
      function(stub)
        .doOnError(e => {
          //          logger.warn(s"[retrying-stub $cluster] received error: $e. Switching channel and retrying...")
          switchChannelEvents.onNext(SwitchChannel(stub))
        })
        .flatMap {
          case notLeaderResponse: NotLeaderRpcResponse =>
            switchChannelEvents.onNext(SwitchChannel(stub, notLeaderResponse.leaderHint))
            Observable.error(new Exception("Not Leader"))
          case response => Observable.just(response)
        }
    }).doOnError(_ => Observable.just(1).delay(Duration(250, MILLISECONDS), ImmediateScheduler()).subscribe())
      .retry
    // TODO: code below causes errors to be thrown on the source observable (leaderStub.take(...));
    // TODO:      possibly due to calling register -> close channel -> send client commands
    // TODO: where should the channel be closed properly?
    //      .doOnCompleted(leaderStub.take(1).subscribe(stub => RaftStubUtils.terminateStubChannel(stub)))
  }

  private def createChannel(leaderHint: Option[ServerAddress] = None): Observable[ManagedChannel] = {
    def createChannel(address: ServerAddress) = address match {
      case TestingAddress(name) => InProcessChannelBuilder.forName(name).build()
      case RemoteAddress(location, port) => NettyChannelBuilder.forAddress(location, port).maxInboundMessageSize(104857600).usePlaintext(true).build()
    }

    if (cluster.isEmpty) return Observable.error(new Exception("Cluster cannot be empty"))
    leaderHint match {
      case None => Observable.just(createChannel(randomClusterServer()))
      case Some(address) => Observable.just(createChannel(address))
    }
  }

  private def createStub(): Observable[RaftClientStub] = createChannel(None).map(RaftClientGrpc.newStub)

  private def switchChannel(event: SwitchChannel): Observable[RaftClientStub] = {
    RaftStubUtils.terminateStubChannel(event.stub)
    createChannel(event.leaderHint).map(RaftClientGrpc.newStub)
  }

  private def randomClusterServer(): ServerAddress = cluster.toSeq(Random.nextInt(cluster.size))

}
