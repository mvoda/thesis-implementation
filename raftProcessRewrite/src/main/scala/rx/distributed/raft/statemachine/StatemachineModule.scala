package rx.distributed.raft.statemachine

import java.io.FileOutputStream

import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging
import rx.distributed.raft.consensus.server.Address.ServerAddress
import rx.distributed.raft.converters.CommandConverters
import rx.distributed.raft.events.Event
import rx.distributed.raft.events.client.{ClientCommandEvent, KeepAliveEvent, RegisterClientEvent}
import rx.distributed.raft.events.rpc.response._
import rx.distributed.raft.events.statemachine._
import rx.distributed.raft.log._
import rx.distributed.raft.objectinputstreams.{ClientClassLoader, ClientJar}
import rx.distributed.raft.statemachine.output.{ForwardingResponse, StatemachineResponse}
import rx.distributed.raft.statemachine.session.StatemachineSessions
import rx.distributed.raft.util.ByteStringSerializer
import rx.lang.scala.schedulers.NewThreadScheduler
import rx.lang.scala.subjects.PublishSubject
import rx.lang.scala.{Observable, Scheduler, Subject}

case class StatemachineModule(address: ServerAddress, statemachine: Statemachine, events: Observable[Event],
                              scheduler: Scheduler = NewThreadScheduler()) extends Logging {
  private val sessions = StatemachineSessions(statemachine.sessionTimeout)

  private val outputSubject: Subject[StatemachineOutputEvent] = PublishSubject()
  val observable: Observable[StatemachineOutputEvent] = outputSubject

  events.collect { case e: CommitLogEntry => e }.observeOn(scheduler).subscribe(commitLogEntry => {
//    logger.info(s"[$address] Statemachine module received log entry: $commitLogEntry")
    sessions.removeExpired(commitLogEntry.entry.timestamp).foreach(client => outputSubject.onNext(DownstreamClientExpired(client)))
    commitLogEntry.entry match {
      case NoOpEntry(_, _) =>
      case LeaderNoOpEntry(_, _) => outputSubject.onNext(BecameLeader(sessions.getCommandSessions))
      case KeepAliveEntry(_, event@KeepAliveEvent(clientId, responseSeqNum, timestamp)) =>
        if (!sessions.registeredClients.contains(clientId)) {
          Observable.just(KeepAliveSessionExpired()).subscribe(event.observer)
        }
        else {
          sessions.keepAlive(clientId, responseSeqNum, timestamp)
          Observable.just(KeepAliveSuccessful()).subscribe(event.observer)
        }
      case RemoveCommandsEntry(_, raftAddress, seqNum, _) =>
        val expiredClientOpt = sessions.removeCommands(raftAddress, seqNum + 1)
        outputSubject.onNext(RemovedCommands(raftAddress, seqNum))
        expiredClientOpt.foreach(client => outputSubject.onNext(DownstreamClientExpired(client)))
      case RegisteredDownstreamClientEntry(_, clusterProducer, registeredId, _) =>
        sessions.registeredDownstreamClient(clusterProducer, registeredId).foreach(outputSubject.onNext(_))
      case RegisterClientEntry(_, clientId, event@RegisterClientEvent(jarsBytes, timestamp)) =>
        sessions.addNewSession(clientId, createClassLoader(clientId, jarsBytes), timestamp)
        Observable.just(RegisterClientSuccessful(clientId)).subscribe(event.observer)
      case ClientLogEntry(_, event@ClientCommandEvent(clientId, seqNum, responseSeqNum, timestamp, clientCommand)) =>
        if (sessions.clientSessionExpired(clientId, seqNum)) {
          Observable.just(ClientResponseSessionExpired()).subscribe(event.observer)
        } else if (sessions.containsResponse(clientId, seqNum)) {
          val serializedResponse = ByteStringSerializer.write(sessions.getResponse(clientId, seqNum))
          Observable.just(ClientResponseSuccessful(seqNum, serializedResponse)).subscribe(event.observer)
        } else {
          val command = CommandConverters.fromByteString(clientCommand, sessions.getClientClassLoader(clientId))
          val output = statemachine.apply(command, commitLogEntry.index)
          sessions.registerDownstreamEvents(clientId, output).foreach(outputSubject.onNext(_))
          sessions.updateSession(clientId, seqNum, responseSeqNum, output, timestamp)
          output match {
            case response: StatemachineResponse =>
              val serializedResponse = ByteStringSerializer.write(response)
              Observable.just(ClientResponseSuccessful(seqNum, serializedResponse)).subscribe(event.observer)
            case forwardingResponse: ForwardingResponse =>
              val serializedResponse = ByteStringSerializer.write(forwardingResponse.response)
              Observable.just(ClientResponseSuccessful(seqNum, serializedResponse)).subscribe(event.observer)
              sessions.resolveCommands(clientId, forwardingResponse.commands).foreach(outputSubject.onNext(_))
          }
        }
      case ExpireSessionsEntry(_, _) =>
    }
  }, throwable => logger.error(s"[$address] Statemachine module input observable threw $throwable"),
    () => logger.info(s"[$address] Statemachine module input observable completed."))

  private def createClassLoader(clientId: String, jarsBytes: Seq[ByteString]): Option[ClientClassLoader] = {
    if (!jarsBytes.exists(_ != ByteString.EMPTY)) {
      None
    } else {
      val clientJarFiles = jarsBytes.map(jarBytes => {
        val jarFile = ClientJar(s"client_$clientId.jar")(address)
        jarBytes.writeTo(new FileOutputStream(jarFile.path))
        jarFile
      })
      Some(ClientClassLoader(clientJarFiles))
    }
  }
}
