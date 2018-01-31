package rx.distributed.raft.rpc.stub

import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging
import rx.distributed.raft.consensus.server.Address.ServerAddress
import rx.distributed.raft.events.rpc.response._
import rx.distributed.raft.rpc.proxy.ClusterStub
import rx.distributed.raft.timeout.RxTimeout
import rx.distributed.raft.{ClientRequestProto, KeepAliveRequestProto}
import rx.lang.scala.Observable
import rx.lang.scala.schedulers.TrampolineScheduler
import rx.lang.scala.subjects.PublishSubject

case class TimeoutRetryingStub(cluster: Set[ServerAddress], timeoutSeconds: Int) extends Logging {
  private val stub = RetryingStub(cluster)

  def registerClient(jarsBytes: Seq[ByteString]): Observable[String] = sendTimedCommand(sendRegisterClient(jarsBytes))

  def keepAlive(requests: Observable[KeepAliveRequestProto]): Observable[KeepAliveResponse] = sendTimedCommand(sendKeepAlive(requests))

  def clientCommands(requests: Observable[ClientRequestProto]): Observable[ClientCommandResponse] = sendTimedCommand(sendClientRequests(requests))

  def clientCommands(requests: Observable[ClientRequestProto], keepAlives: Observable[KeepAliveRequestProto]): Observable[ClientCommandResponse] = {
    sendTimedCommand(keepAliveAndSendRequests(requests, keepAlives))
  }

  private def sendTimedCommand[T](commandLambda: (RxTimeout) => Observable[T]): Observable[T] = {
    val timeout = RxTimeout(timeoutSeconds * 1000)
    timeout.start()
    timeout.flatMap(_ => Observable.error(timeoutException))
      .merge(commandLambda(timeout).subscribeOn(TrampolineScheduler()).doOnCompleted(timeout.onCompleted()))
  }

  private def sendRegisterClient(jarsBytes: Seq[ByteString])(timeout: RxTimeout): Observable[String] = {
    stub.leaderRequest(leaderStub => ClusterStub.registerClient(leaderStub, jarsBytes).doOnNext(_ => timeout.reset())).flatMap {
      case RegisterClientSuccessful(clientId) => Observable.just(clientId)
      case _ => Observable.empty
    }
  }

  private def sendKeepAlive(requests: Observable[KeepAliveRequestProto])(timeout: RxTimeout): Observable[KeepAliveResponse] = {
    stub.leaderRequest(leaderStub => requests.flatMap(request => ClusterStub.keepAlive(leaderStub, request)).doOnNext(_ => timeout.reset()))
  }

  private def sendClientRequests(requests: Observable[ClientRequestProto])(timeout: RxTimeout): Observable[ClientCommandResponse] = {
    stub.leaderRequest(leaderStub => ClusterStub.clientRequest(leaderStub, requests).doOnNext(_ => timeout.reset()))
  }

  private def keepAliveAndSendRequests(requests: Observable[ClientRequestProto], keepAlives: Observable[KeepAliveRequestProto])
                                      (timeout: RxTimeout): Observable[ClientCommandResponse] = {
    val stopTrigger = PublishSubject[Int]()
    val sessionExpiredResponses = sendKeepAlive(keepAlives)(timeout)
      .collect { case response: KeepAliveSessionExpired => response }
      .map(_ => ClientResponseSessionExpired())
      .takeUntil(stopTrigger)
    sessionExpiredResponses.merge(sendClientRequests(requests)(timeout).doOnCompleted(stopTrigger.onNext(0)))
  }

  private val timeoutException = new Exception(s"Failed to connect to any cluster server in $timeoutSeconds seconds")
}
