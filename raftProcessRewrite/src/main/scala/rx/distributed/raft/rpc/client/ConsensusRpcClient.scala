package rx.distributed.raft.rpc.client

import java.util.concurrent.atomic.AtomicBoolean

import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.{Channel, ManagedChannelBuilder}
import org.apache.logging.log4j.scala.Logging
import rx.distributed.raft._
import rx.distributed.raft.consensus.server.Address.{RemoteAddress, ServerAddress, TestingAddress}
import rx.distributed.raft.converters.ObserverConverters
import rx.distributed.raft.events.Event
import rx.distributed.raft.events.consensus.{AppendEntries, VoteRequest}
import rx.distributed.raft.events.internal.{AppendEntriesToServer, CountingAppendEntriesToServer, RequestVotesFromServer, RpcClientEvent}
import rx.distributed.raft.events.rpc.response.{AppendEntriesResponse, VoteResponse}
import rx.distributed.raft.util.RaftStubUtils
import rx.lang.scala.schedulers.ImmediateScheduler
import rx.lang.scala.subjects.PublishSubject
import rx.lang.scala.{Observable, Subject, Subscriber}

import scala.collection.mutable
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.util.Try

case class ConsensusRpcClient(address: ServerAddress, events: Observable[Event]) extends Logging {
  private val clientRequests = events.collect { case e: RpcClientEvent => e }
  private val outputSubject: Subject[Event] = PublishSubject().toSerialized
  private val stubs: mutable.Map[ServerAddress, RaftGrpc.RaftStub] = mutable.Map[ServerAddress, RaftGrpc.RaftStub]()
  private val completed = new AtomicBoolean(false)
  val observable: Observable[Event] = outputSubject

  clientRequests.subscribe(event => handleClientEvent(event),
    throwable => logger.error(s"[$address] Consensus RPC client input observable threw: $throwable"),
    () => {
      stubs.values.foreach(stub => RaftStubUtils.terminateStubChannel(stub))
      logger.info(s"[$address] Consensus RPC client input observable completed.")
      completed.compareAndSet(false, true)
    })


  def handleClientEvent(event: RpcClientEvent): Unit = {
//    logger.info(s"[$address] RpcClient received event: $event")
    event match {
      case RequestVotesFromServer(targetAddress, request) => requestVoteToServer(targetAddress, request)
        .subscribe(response => outputSubject.onNext(response))
      case CountingAppendEntriesToServer(AppendEntriesToServer(targetAddress, request), activeLeaderObserver) =>
        appendEntriesToServer(targetAddress, request).subscribe(response => {
          activeLeaderObserver.foreach(observer => observer.onNext(response))
          outputSubject.onNext(response)
        })
    }
  }

  private def createChannel(address: ServerAddress): Channel = address match {
    case TestingAddress(name) => InProcessChannelBuilder.forName(name).build()
    case RemoteAddress(location, port) => ManagedChannelBuilder.forAddress(location, port).usePlaintext(true).build()
  }

  private def getStub(address: ServerAddress): RaftGrpc.RaftStub = stubs.getOrElseUpdate(address, {
    RaftGrpc.newStub(createChannel(address))
  })

  def requestVoteToServer(address: ServerAddress, request: VoteRequest): Observable[VoteResponse] = {
    val stub = getStub(address)
    Observable((subscriber: Subscriber[VoteResponseProto]) =>
      Try(stub.requestVote(request.toProtobuf, ObserverConverters.fromRx(subscriber)))
        .recover { case e => subscriber.onError(e) })
      .map(VoteResponse(_))
      .doOnError(_ => Observable.just(1).delay(Duration(100, MILLISECONDS), ImmediateScheduler()).subscribe())
      .retry((_, _) => !completed.get())
      .onErrorResumeNext(_ => Observable.empty)
  }

  def appendEntriesToServer(address: ServerAddress, request: AppendEntries): Observable[AppendEntriesResponse] = {
    val stub = getStub(address)
    Observable((subscriber: Subscriber[AppendEntriesResponseProto]) =>
      Try(stub.appendEntries(request.toProtobuf, ObserverConverters.fromRx(subscriber)))
        .recover { case e => subscriber.onError(e) })
      .map(AppendEntriesResponse(_))
      .doOnError(_ => Observable.just(1).delay(Duration(100, MILLISECONDS), ImmediateScheduler()).subscribe())
      .retry((_, _) => !completed.get())
      .onErrorResumeNext(_ => Observable.empty)
  }
}
