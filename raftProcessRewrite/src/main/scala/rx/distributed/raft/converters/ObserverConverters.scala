package rx.distributed.raft.converters

import io.grpc.stub.StreamObserver
import org.apache.logging.log4j.scala.Logging
import rx.distributed.raft._
import rx.distributed.raft.events.rpc.response._
import rx.lang.scala.JavaConversions._
import rx.lang.scala.{Observer, Subject, Subscriber}
import rx.observers.SafeSubscriber

import scala.util.Try

object ObserverConverters extends Logging {
  def fromAppendEntriesStreamObserver(observer: StreamObserver[AppendEntriesResponseProto]): Observer[AppendEntriesResponse] =
    fromProto(observer, _.toProtobuf)

  def fromVoteStreamObserver(observer: StreamObserver[VoteResponseProto]): Observer[VoteResponse] =
    fromProto(observer, _.toProtobuf)

  def fromRegisterClientStreamObserver(observer: StreamObserver[RegisterClientResponseProto]): Observer[RegisterClientResponse] =
    fromProto(observer, _.toProtobuf)

  def fromKeepAliveStreamObserver(observer: StreamObserver[KeepAliveResponseProto]): Observer[KeepAliveResponse] =
    fromProto(observer, _.toProtobuf)

  def fromClientResponseStreamObserver(observer: StreamObserver[ClientResponseProto]): Observer[ClientCommandResponse] =
    fromProto(observer, _.toProtobuf)

  def fromProto[T](observer: StreamObserver[T]): Observer[T] =
    fromProto(observer, identity)

  // wrap calls in try to avoid throwing exceptions if leader fails while a response is sent
  def fromProto[T, R](observer: StreamObserver[T], mapper: R => T): Observer[R] =
    makeSafe(Subscriber(
      response => Try(observer.onNext(mapper(response))),
      throwable => Try(observer.onError(throwable)),
      () => Try(observer.onCompleted())))

  def fromRx[T](subject: Subject[T]): StreamObserver[T] =
    new StreamObserver[T] {
      override def onError(throwable: Throwable): Unit = subject.onError(throwable)

      override def onCompleted(): Unit = subject.onCompleted()

      override def onNext(t: T): Unit = subject.onNext(t)
    }

  def fromRx[T](subscriber: Subscriber[T]): StreamObserver[T] =
    new StreamObserver[T] {
      private val safeSubscriber = makeSafe(subscriber)

      override def onError(throwable: Throwable): Unit = safeSubscriber.onError(throwable)

      override def onCompleted(): Unit = safeSubscriber.onCompleted()

      override def onNext(t: T): Unit = safeSubscriber.onNext(t)
    }

  def makeSafe[T](subscriber: Subscriber[T]): Subscriber[T] = toScalaSubscriber(new SafeSubscriber[T](toJavaSubscriber(subscriber)))
}
