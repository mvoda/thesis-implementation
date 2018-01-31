package rx.distributed.raft.rpc.client

import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging
import rx.distributed.raft.KeepAliveRequestProto
import rx.distributed.raft.consensus.server.Address.ServerAddress
import rx.distributed.raft.events.rpc.response.{ClientResponseSessionExpired, ClientResponseSuccessful}
import rx.distributed.raft.rpc.stub.TimeoutRetryingStub
import rx.distributed.raft.statemachine.input.StatemachineCommand
import rx.distributed.raft.statemachine.output.StatemachineResponse
import rx.distributed.raft.timeout.RxTimeout
import rx.distributed.raft.util.{CommandQueue, IndexedCommand, IndexedResponse, ResponseTracker}
import rx.lang.scala.Observable
import rx.lang.scala.subjects.PublishSubject

case class RaftClient(cluster: Set[ServerAddress], timeoutSeconds: Int, keepAliveTimeoutSeconds: Int, jarsBytes: Seq[ByteString]) extends Logging {
  def register(): Observable[String] = {
    val proxy = TimeoutRetryingStub(cluster, timeoutSeconds)
    proxy.registerClient(jarsBytes)
  }

  def sendCommands(commands: Observable[StatemachineCommand]): Observable[StatemachineResponse] = {
    val proxy = TimeoutRetryingStub(cluster, timeoutSeconds)
    proxy.registerClient(jarsBytes).take(1).flatMap(clientId => {
      val (responses, tracker) = sendCommands(clientId, commands, 0, 0, proxy)
      responses.doOnNext(indexedResponse => tracker.addResponse(indexedResponse.index))
    }).map(_.response)
  }

  def sendCommands(clientId: String, commands: Observable[StatemachineCommand],
                   startingSeqNum: Int = 0, startingResponseSeqNum: Int = 0,
                   proxy: TimeoutRetryingStub = TimeoutRetryingStub(cluster, timeoutSeconds)): (Observable[IndexedResponse], ResponseTracker) = {
    val commandQueue = CommandQueue(commands.zipWithIndex.map(pair => IndexedCommand(pair._2 + startingSeqNum, pair._1)).share)
    val responseTracker = ResponseTracker(commandQueue, startingResponseSeqNum)
    val keepAliveTimeout = RxTimeout(keepAliveTimeoutSeconds * 1000, keepAliveTimeoutSeconds * 1000)
    val keepAlives = PublishSubject[KeepAliveRequestProto]()
    keepAliveTimeout.map(_ => createKeepAliveRequest(clientId, responseTracker.get())).subscribe(keepAlives)
    val clientCommands = commandQueue.commands.map(_.toProtobuf(clientId, responseTracker.get()))
    val responseObservable = proxy.clientCommands(clientCommands, keepAlives)
      .flatMap {
        case ClientResponseSuccessful(seqNum, response) => Observable.just(IndexedResponse(seqNum, response))
        case ClientResponseSessionExpired() =>
          logger.error(s"Session expired for client: $clientId at responseSeqNum: ${responseTracker.get()}")
          Observable.error(new Exception("Session expired"))
      }.doOnSubscribe(keepAliveTimeout.start())
      .doOnCompleted(keepAliveTimeout.onCompleted())
      .doOnUnsubscribe {
        commandQueue.unsubscribe()
        keepAliveTimeout.onCompleted()
      }
    (responseObservable, responseTracker)
  }

  private def createKeepAliveRequest(clientId: String, responseSeqNum: Int = 0) =
    KeepAliveRequestProto.newBuilder().setClientId(clientId).setResponseSequenceNum(responseSeqNum).build()
}
