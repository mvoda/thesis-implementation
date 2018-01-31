package rx.distributed.rpc

import java.util.concurrent.atomic.AtomicInteger

import io.grpc.Server
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.netty.NettyServerBuilder
import org.apache.logging.log4j.scala.Logging
import rx.distributed.notifications.{RxOnCompleted, RxOnError, RxOnNext}
import rx.distributed.raft.consensus.server.Address.{RemoteAddress, ServerAddress, TestingAddress}
import rx.distributed.raft.converters.ObserverConverters
import rx.distributed.raft.events.client.{ClientCommandEvent, KeepAliveEvent, RegisterClientEvent}
import rx.distributed.raft.events.rpc.response.{ClientResponseSuccessful, KeepAliveSuccessful, RegisterClientSuccessful}
import rx.distributed.raft.rpc.client.RaftClient
import rx.distributed.raft.rpc.server.CommandRPCServer
import rx.distributed.raft.statemachine.input.StatemachineCommand
import rx.distributed.raft.util.{ByteStringDeserializer, ByteStringSerializer}
import rx.distributed.statemachine.commands.{RxEvent, SetUpstreamForStreamId, Unsubscribe}
import rx.distributed.statemachine.responses.Empty
import rx.lang.scala.subscriptions.CompositeSubscription
import rx.lang.scala.{Observable, Subscriber, Subscription}

import scala.collection.mutable

//TODO: simplify client rpc server
class RPCServer(val address: ServerAddress) extends Logging {
  var started = false
  val count = new AtomicInteger(0)
  val observers: mutable.Map[String, Subscriber[Any]] = mutable.Map()
  val subscriptions: mutable.Map[String, CompositeSubscription] = mutable.Map()
  val clientServer = CommandRPCServer()

  val server: Server = address match {
    case TestingAddress(name) =>
      InProcessServerBuilder.forName(name).addService(clientServer).build()
    case RemoteAddress(_, port) =>
      NettyServerBuilder.forPort(port).maxMessageSize(104857600).addService(clientServer).build()
  }

  private val subscription = clientServer.observable.subscribe(_ match {
    case event@ClientCommandEvent(clientId, seqNum, _, _, commandByteString) =>
      val command = ByteStringDeserializer.deserialize[StatemachineCommand](commandByteString)
      //      logger.info(s"[Client ($address)] server received command: $command from: $clientId")
      val serializedResponse = ByteStringSerializer.write(Empty())
      Observable.just(ClientResponseSuccessful(seqNum, serializedResponse)).subscribe(event.observer)
      command match {
        case rxEvent: RxEvent[_] => handleRxEvent(rxEvent)
        case setUpstream: SetUpstreamForStreamId => handleSetUpstream(setUpstream)
        case _ =>
        //          logger.info(s"[Client ($address)] received OTHER command: " + other)
      }
    case event@RegisterClientEvent(_, _) =>
      //      logger.info(s"client server($address) RECEIVED REGISTER CLIENT: $event")
      val clientId = count.getAndIncrement().toString
      Observable.just(RegisterClientSuccessful(clientId)).subscribe(event.observer)
    case event@KeepAliveEvent(_, _, _) =>
      Observable.just(KeepAliveSuccessful()).subscribe(event.observer)
  }, e => logger.error(s"[Client ($address)] server observable error: $e"),
    () => logger.info(s"[Client ($address)] server observable completed"))

  private def handleRxEvent(rxEvent: RxEvent[_]): Unit = {
    observers.get(rxEvent.streamId).foreach(observer => rxEvent.notification match {
      case RxOnNext(value) => observer.onNext(value)
      case RxOnError(throwable) => observer.onError(throwable)
      case RxOnCompleted() => observer.onCompleted()
    })
  }

  private def handleSetUpstream(setUpstream: SetUpstreamForStreamId): Unit = {
    //    logger.info(s"[Client ($address)] stream (${setUpstream.streamId}) will unsubscribe from: " +
    //      s"[${setUpstream.upstreamCluster}, ${setUpstream.upstreamStreamId}]")
    observers.get(setUpstream.streamId) match {
      case Some(_) =>
        val newSubscription: CompositeSubscription = subscriptions(setUpstream.streamId) += Subscription {
          sendUnsubscribe(setUpstream)
        }
        subscriptions.update(setUpstream.streamId, newSubscription)
      case None =>
        sendUnsubscribe(setUpstream)
    }
  }

  private def sendUnsubscribe(unsubscribeFrom: SetUpstreamForStreamId): Unit = {
    val unsubscribeMessage = Unsubscribe(unsubscribeFrom.upstreamStreamId)
    //    logger.info(s"[Client ($address)] stream (${unsubscribeFrom.streamId}) sending $unsubscribeMessage to " +
    //      s"[${unsubscribeFrom.upstreamCluster}, ${unsubscribeFrom.upstreamStreamId}]")
    val raftClient = RaftClient(unsubscribeFrom.upstreamCluster, 30, 1, Seq())
    raftClient.sendCommands(Observable.just(unsubscribeMessage)).toBlocking.subscribe()
  }

  def generateId[T](subscriber: Subscriber[T]): String = {
    val streamId = s"${count.getAndIncrement()}"
    val anySubscriber = makeAnySubscriber(subscriber)
    observers.put(streamId, anySubscriber)
    val subscription = CompositeSubscription(subscriber) += Subscription {
      observers.remove(streamId)
      subscriptions.remove(streamId)
    }
    subscriptions.put(streamId, subscription)
    subscriber.add(subscription)
    streamId
  }

  private def makeAnySubscriber[T](subscriber: Subscriber[T]): Subscriber[Any] = {
    ObserverConverters.makeSafe(Subscriber(
      (value: Any) => subscriber.onNext(value.asInstanceOf[T]),
      (throwable: Throwable) => subscriber.onError(throwable),
      () => subscriber.onCompleted()
    ))
  }

  def start(): Unit = {
    this.synchronized {
      if (!started) {
        server.start()
        started = true
        logger.info(s"Started client rpc server on: $address")
      }
    }
  }

  def shutdown(): Unit = {
    this.synchronized {
      if (started) {
        subscription.unsubscribe()
        server.shutdownNow()
        server.awaitTermination()
        started = false
      }
    }
  }
}
