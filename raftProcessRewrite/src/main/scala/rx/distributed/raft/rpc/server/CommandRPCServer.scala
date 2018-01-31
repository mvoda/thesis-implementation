package rx.distributed.raft.rpc.server

import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver
import org.apache.logging.log4j.scala.Logging
import rx.distributed.raft._
import rx.distributed.raft.converters.{ObserverConverters, ProtobufConverters}
import rx.distributed.raft.events.client.{KeepAliveEvent, RegisterClientEvent}
import rx.distributed.raft.events.rpc.request.ClientRpcRequest
import rx.distributed.raft.events.rpc.response.ClientCommandResponse
import rx.lang.scala.subjects.{PublishSubject, ReplaySubject}
import rx.lang.scala.{Observable, Subject}

import scala.collection.JavaConverters._

case class CommandRPCServer() extends RaftClientGrpc.RaftClientImplBase with RPCServer[ClientRpcRequest] with Logging {
  private val eventSubject: Subject[ClientRpcRequest] = PublishSubject().toSerialized
  override val observable: Observable[ClientRpcRequest] = eventSubject

  override def registerClient(request: RegisterClientRequestProto, responseObserver: StreamObserver[RegisterClientResponseProto]): Unit = {
    eventSubject.onNext(RegisterClientEvent(getJars(request), System.currentTimeMillis())(ObserverConverters.fromRegisterClientStreamObserver(responseObserver)))
  }

  override def clientRequest(responseObserver: StreamObserver[ClientResponseProto]): StreamObserver[ClientRequestProto] = {
    val stream = ReplaySubject[ClientRequestProto]()
    stream.flatMap(clientRequest => {
      Observable[ClientCommandResponse](observer => {
        eventSubject.onNext(ProtobufConverters.fromProtobuf(clientRequest, observer))
      })
    }).subscribe(ObserverConverters.fromClientResponseStreamObserver(responseObserver))
    ObserverConverters.fromRx(stream)
  }

  override def keepAlive(request: KeepAliveRequestProto, responseObserver: StreamObserver[KeepAliveResponseProto]): Unit = {
    eventSubject.onNext(KeepAliveEvent(request.getClientId, request.getResponseSequenceNum, System.currentTimeMillis())
    (ObserverConverters.fromKeepAliveStreamObserver(responseObserver)))
  }

  private def getJars(request: RegisterClientRequestProto): Seq[ByteString] = {
    request.getJarsList.asScala.filter(_ != ByteString.EMPTY)
  }
}
