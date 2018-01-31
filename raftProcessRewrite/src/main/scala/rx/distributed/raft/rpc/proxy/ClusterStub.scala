package rx.distributed.raft.rpc.proxy

import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging
import rx.distributed.raft.RaftClientGrpc.RaftClientStub
import rx.distributed.raft._
import rx.distributed.raft.converters.ObserverConverters
import rx.distributed.raft.events.rpc.response.{ClientCommandResponse, KeepAliveResponse, RegisterClientResponse}
import rx.lang.scala.subscriptions.CompositeSubscription
import rx.lang.scala.{Observable, Subscriber}

import scala.collection.JavaConverters._
import scala.util.Try

object ClusterStub extends Logging {
  def registerClient(stub: RaftClientStub, jarsBytes: Seq[ByteString]): Observable[RegisterClientResponse] = {
    Observable((subscriber: Subscriber[RegisterClientResponseProto]) =>
      Try(stub.registerClient(registerClientCommand(jarsBytes), ObserverConverters.fromRx(subscriber)))
        .recover { case e => subscriber.onError(e) })
      .map(protoResponse => RegisterClientResponse(protoResponse))
  }

  def keepAlive(stub: RaftClientStub, request: KeepAliveRequestProto): Observable[KeepAliveResponse] = {
    Observable((subscriber: Subscriber[KeepAliveResponseProto]) =>
      Try(stub.keepAlive(request, ObserverConverters.fromRx(subscriber)))
        .recover { case e => subscriber.onError(e) })
      .map(protoResponse => KeepAliveResponse(protoResponse))
  }

  //TODO: figure out why sometimes we get duplicate events
  def clientRequest(stub: RaftClientStub, commands: Observable[ClientRequestProto]): Observable[ClientCommandResponse] = {
    Observable((subscriber: Subscriber[ClientResponseProto]) => {
      val stubSubscription = CompositeSubscription()
      Try({
        val subscription = commands
          .subscribe(ObserverConverters.fromProto(stub.clientRequest(ObserverConverters.fromRx(subscriber))))
        stubSubscription += subscription
        subscriber.add(subscription)
      })
        .recover { case e => {
          stubSubscription.unsubscribe()
          subscriber.onError(e)
        }
        }
    })
      .map(protoResponse => ClientCommandResponse(protoResponse))
  }

  private def registerClientCommand(jarsBytes: Seq[ByteString]): RegisterClientRequestProto = {
    RegisterClientRequestProto.newBuilder().addAllJars(jarsBytes.asJava).build()
  }
}
