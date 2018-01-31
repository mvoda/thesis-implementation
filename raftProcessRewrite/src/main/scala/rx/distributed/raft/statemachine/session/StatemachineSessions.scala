package rx.distributed.raft.statemachine.session

import org.apache.logging.log4j.scala.Logging
import rx.distributed.raft.client.RaftClusterClient
import rx.distributed.raft.events.statemachine.{RegisterDownstreamClient, SendDownstream}
import rx.distributed.raft.objectinputstreams.ClientClassLoader
import rx.distributed.raft.statemachine.input.DownstreamCommand
import rx.distributed.raft.statemachine.output.{ForwardingResponse, StatemachineOutput, StatemachineResponse}

import scala.collection.immutable.{HashMap, SortedMap}
import scala.reflect.internal.util.ScalaClassLoader

case class StatemachineSessions(sessionTimeout: Int) extends Logging {
  private var responseSessions: Map[String, ResponseSession] = HashMap()
  val commandSessions: CommandSessions = CommandSessions()

  def addNewSession(clientId: String, classLoader: Option[ClientClassLoader], timestamp: Long): Unit = {
    responseSessions = responseSessions + (clientId -> ResponseSession(classLoader, SortedMap(), 0, timestamp))
  }

  def keepAlive(clientId: String, responseSeqNum: Int, timestamp: Long): Unit = {
    val clientSession = responseSessions(clientId)
    responseSessions = responseSessions + (clientId -> clientSession.keepAlive(responseSeqNum, timestamp))
  }

  def updateSession(clientId: String, seqNum: Int, responseSeqNum: Int, output: StatemachineOutput, timestamp: Long): Unit = {
    val clientSession = responseSessions(clientId)
    output match {
      case response: StatemachineResponse =>
        responseSessions = responseSessions + (clientId -> clientSession.update(seqNum, responseSeqNum, response, timestamp))
      case forwardingResponse: ForwardingResponse =>
        responseSessions = responseSessions + (clientId -> clientSession.update(seqNum, responseSeqNum, forwardingResponse.response, timestamp))
        commandSessions.update(clientProducer(clientId), forwardingResponse.commands)
    }
  }

  // removes expired responseSessions and commandSessions without producers and commands
  // returns the set of RaftClusters that have no more producers and no more commands
  def removeExpired(timestamp: Long): Set[RaftClusterClient] = {
    val aliveResponseSessions = expireResponseSessions(timestamp, responseSessions)
    val expiredProducers = responseSessions.filterKeys(!aliveResponseSessions.contains(_)).keySet
    val expiredRafClusterClients = commandSessions.removeExpiredSessions(expiredProducers)
    responseSessions = aliveResponseSessions
    expiredRafClusterClients
  }

  private def expireResponseSessions(timestamp: Long, sessions: Map[String, ResponseSession]): Map[String, ResponseSession] = {
    sessions.filter { case (_, session) => timestamp - session.timestamp < sessionTimeout }
  }

  def removeCommands(downstreamClient: RaftClusterClient, seqNum: Int): Option[RaftClusterClient] =
    commandSessions.removeCommands(downstreamClient, seqNum)

  def registerDownstreamEvents(clientId: String, output: StatemachineOutput): Seq[RegisterDownstreamClient] = {
    output match {
      case _: StatemachineResponse => Seq()
      case forwardingResponse: ForwardingResponse =>
        commandSessions.registerDownstreamEvents(clientProducer(clientId), forwardingResponse.commands)
    }
  }

  def registeredDownstreamClient(clusterProducer: RaftClusterProducer, registeredId: String): Seq[SendDownstream] =
    commandSessions.registeredDownstreamClient(clusterProducer, registeredId)

  def resolveCommands(clientId: String, commands: Seq[DownstreamCommand]): Seq[SendDownstream] =
    commandSessions.resolveCommands(clientProducer(clientId), commands)

  def registeredClients: Set[String] = responseSessions.keySet

  def clientSessionExpired(clientId: String, seqNum: Int): Boolean =
    !registeredClients.contains(clientId) || responseSessions(clientId).isDiscarded(seqNum)

  def containsResponse(clientId: String, seqNum: Int): Boolean = registeredClients.contains(clientId) && responseSessions(clientId).saved(seqNum)

  def getResponse(clientId: String, seqNum: Int): StatemachineResponse = responseSessions(clientId).getResponse(seqNum)

  def getClientClassLoader(clientId: String): Option[ScalaClassLoader] = responseSessions(clientId).classLoader.map(_.classLoader)

  def getCommandSessions: CommandSessions = commandSessions

  private def clientProducer(clientId: String): ClientProducer =
    ClientProducer(clientId, responseSessions(clientId).classLoader.map(_.jars).getOrElse(Seq()))
}
