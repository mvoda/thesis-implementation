package rx.distributed.statemachine

import org.apache.logging.log4j.scala.Logging
import rx.distributed.notifications.RxNotification
import rx.distributed.observers.notifications.NotificationObserver
import rx.distributed.operators._
import rx.distributed.operators.combining.{CombiningOperator, LeftStream, RightStream}
import rx.distributed.operators.errorHandling.ErrorHandlingOperator
import rx.distributed.operators.single.SingleOperator
import rx.distributed.raft.statemachine.Statemachine
import rx.distributed.raft.statemachine.input.{DownstreamCommand, StatemachineCommand}
import rx.distributed.raft.statemachine.output.{ForwardingResponse, StatemachineOutput, StatemachineResponse}
import rx.distributed.statemachine.commands.{RxEvent, SetUpstreamForStreamId, Subscribe, Unsubscribe}
import rx.distributed.statemachine.exceptions.SetupException
import rx.distributed.statemachine.responses.{Empty, Forwarding, ForwardingEmpty, Unsubscribed}
import rx.lang.scala.Observable

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

case class RxStatemachine(sessionTimeout: Int) extends Statemachine with Logging {

  //TODO: split streams into streams (streamId, operators) and subscribed streams?
  val streams: mutable.Map[String, RxStream] = mutable.Map()
  val partialStreams: mutable.Map[String, RxStream] = mutable.Map()
  val unsubscribeMessages: mutable.Map[String, ForwardingResponse] = mutable.Map()

  override def apply(command: StatemachineCommand): StatemachineOutput = {
//    logger.info(s"Statemachine received command: $command")
    command match {
      case Subscribe(streamId, operators, statemachineObserver) =>
        val observer = NotificationObserver(statemachineObserver)
        handleSubscribe(streamId, operators, observer)
        observer.output
      case RxEvent(streamId, notification) =>
        val rxEventOutput = streams.get(streamId) match {
          case Some(rxStream) =>
            if (!rxStream.isUnsubscribed) {
              rxStream.inputObserver.onNext(notification)
              rxStream.output
            } else {
              Unsubscribed(streamId)
            }
          case None => Unsubscribed(streamId)
        }
        removeUnsubscribedStreams(rxEventOutput)
      case Unsubscribe(streamId) => unsubscribeStream(streamId)
      case SetUpstreamForStreamId(streamId, upstreamCluster, upstreamStreamId) =>
        val unsubscribeMessage = ForwardingEmpty(upstreamCluster, Seq(Unsubscribe(upstreamStreamId)))
        streams.get(streamId) match {
          case None => unsubscribeMessage
          case Some(_) =>
            unsubscribeMessages.put(streamId, unsubscribeMessage)
            Empty()
        }
    }
  }

  private def createStream(rxStream: RxStream, operators: Seq[Operator]): Try[Observable[Any]] = {
    operators.foldRight[Try[Observable[Any]]](Success(rxStream.streamInput.observable))((op, tryObservable) => {
      op match {
        case singleOp: SingleOperator[_, _] => tryObservable.flatMap(obs => Try(singleOp.toObservable(obs)))
        case doubleOp: CombiningOperator[_, _, _] =>
          val operatorId = doubleOp.operatorId
          println(s"Create Stream, first time subscribing: $doubleOp")
          val otherStream = RxStream(rxStream.notificationObserver)
          partialStreams.put(operatorId, otherStream)
          doubleOp.streamPosition match {
            case LeftStream() => tryObservable.flatMap(obs => Try(doubleOp.toObservable(obs, otherStream.streamInput.observable)))
            case RightStream() => tryObservable.flatMap(obs => Try(doubleOp.toObservable(otherStream.streamInput.observable, obs)))
          }
        case errorHandlingOp: ErrorHandlingOperator[_, _, _] =>
          val onError = (t: Throwable) => {
            val (resumeStreamId, resumeOperators) = errorHandlingOp.onError(t)
            val onErrorStream = RxStream(rxStream.notificationObserver)
            streams.put(resumeStreamId, onErrorStream)
            createStream(onErrorStream, resumeOperators).get
          }
          tryObservable.flatMap(obs => Try(errorHandlingOp.toObservable(obs, onError)))
      }
    })
  }

  private def handleSubscribe(streamId: String, operators: Seq[Operator], notificationObserver: NotificationObserver[Any]) = {
    if (streams.contains(streamId)) {
      notificationObserver.subscriber.onError(SetupException(s"Input with id: $streamId already exists on this statemachine."))
    } else {
      val newOperators = findNewOperators(operators)
      val rxStream = resolveStream(operators, newOperators, notificationObserver)
      createStream(rxStream, newOperators) match {
        case Success(observable) =>
          observable.materialize.map(RxNotification(_)).subscribe(rxStream.outputObserver)
          streams.put(streamId, rxStream)
        case Failure(e) => notificationObserver.subscriber.onError(SetupException(e.getMessage))
      }
    }
  }

  private def resolveStream(operators: Seq[Operator], newOperators: Seq[Operator], notificationObserver: NotificationObserver[Any]): RxStream = {
    if (newOperators.size < operators.size) {
      val downstreamId = findCombinerId(operators, newOperators)
      val downstream = partialStreams.remove(downstreamId).get
      RxStream(downstream)
    }
    else RxStream(notificationObserver)
  }

  private def findCombinerId(operators: Seq[Operator], newOperators: Seq[Operator]): String = {
    assert(operators.size > newOperators.size)
    val combiner = operators(newOperators.size)
    combiner.asInstanceOf[CombiningOperator[_, _, _]].operatorId
  }

  private def findNewOperators(operators: Seq[Operator]): Seq[Operator] = {
    operators.takeWhile {
      case combiner: CombiningOperator[_, _, _] => !partialStreams.contains(combiner.operatorId)
      case _ => true
    }
  }

  private def removeUnsubscribedStreams(processedOutput: StatemachineOutput): StatemachineOutput = {
    val unsubscribeCommands = streams.filter { case (_, rxStream) => rxStream.isUnsubscribed }.keys.map(unsubscribeStream)
      .foldLeft(Seq[DownstreamCommand]())((downstreamCommands, statemachineOutput) => statemachineOutput match {
        case forwardingResponse: ForwardingResponse => downstreamCommands ++ forwardingResponse.commands
        case _ => downstreamCommands
      })
    if (unsubscribeCommands.isEmpty) processedOutput
    else processedOutput match {
      case response: StatemachineResponse => Forwarding(response, unsubscribeCommands)
      case forwardingResponse: ForwardingResponse => ForwardingEmpty(forwardingResponse.commands ++ unsubscribeCommands)
    }
  }

  private def unsubscribeStream(streamId: String): StatemachineOutput = {
    streams.get(streamId) match {
      case Some(_) =>
        streams.remove(streamId)
        unsubscribeMessages.remove(streamId) match {
          case Some(message) => message
          case None => Empty()
        }
      case None => Empty()
    }
  }
}
