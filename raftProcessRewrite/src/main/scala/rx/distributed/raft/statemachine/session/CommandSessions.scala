package rx.distributed.raft.statemachine.session

import org.apache.logging.log4j.scala.Logging
import rx.distributed.raft.client.RaftClusterClient
import rx.distributed.raft.events.statemachine.{RegisterDownstreamClient, SendDownstream}
import rx.distributed.raft.statemachine.input.DownstreamCommand

import scala.collection.immutable.HashMap

case class CommandSessions() extends Logging {
  private var commandSessions: Map[RaftClusterProducer, DownstreamCommandSession] = HashMap()

  def update(clientProducer: ClientProducer, downstreamCommands: Seq[DownstreamCommand]): Unit = {
    downstreamCommands.foreach { case DownstreamCommand(cluster, command) =>
      val clusterProducer = RaftClusterProducer(cluster, clientProducer)
      val cmdSession = if (commandSessions.contains(clusterProducer)) commandSessions(clusterProducer).addCommand(command)
      else DownstreamCommandSession(producerExpired = false, None, 0, Seq(command))
      commandSessions = commandSessions + (clusterProducer -> cmdSession)
    }
  }

  /**
    * Remove delivered commands from the command session associated with this downstream raft client.
    *
    * @param downstreamClient downstream client that delivered the command
    * @param seqNum           command sequence number up to which commands can be discarded (non-iclusive)
    * @return the downstream client if it has finished delivering commands (producer is expired and pending commands are empty), None otherwise
    */

  def removeCommands(downstreamClient: RaftClusterClient, seqNum: Int): Option[RaftClusterClient] = {
    val clusterProducers = commandSessions.filter { case (producer, session) =>
      producer.cluster == downstreamClient.cluster && session.clientId.contains(downstreamClient.id)
    }.keys
    assert(clusterProducers.size == 1)
    val clusterProducer = clusterProducers.head
    val cmdSession = commandSessions(clusterProducer).removeCommands(seqNum)
    if (cmdSession.isCompleted) {
      commandSessions = commandSessions - clusterProducer
      Some(downstreamClient)
    }
    else {
      commandSessions = commandSessions + (clusterProducer -> cmdSession)
      None
    }
  }

  // removes commandSessions without alive producers and commands
  // returns the set of RaftClusters that have no more producers and no more commands
  def removeExpiredSessions(expiredProducers: Set[String]): Set[RaftClusterClient] = {
    expireProducers(expiredProducers)
    val (completedSessions, aliveSessions) = commandSessions.partition(_._2.isCompleted)
    commandSessions = aliveSessions
    completedSessions.map { case (clusterProducer, session) => RaftClusterClient(clusterProducer.cluster, session.clientId.get) }.toSet
  }

  private def expireProducers(expiredProducers: Set[String]): Unit = {
    commandSessions = commandSessions.map { case (clusterProducer, session) =>
      if (expiredProducers.contains(clusterProducer.producer.clientId)) (clusterProducer, session.expire())
      else (clusterProducer, session)
    }
  }

  def producers: Set[RaftClusterProducer] = commandSessions.keySet

  def commandSession(producer: RaftClusterProducer) = commandSessions(producer)

  def registeredDownstreamClient(clusterProducer: RaftClusterProducer, registeredId: String): Seq[SendDownstream] = {
    commandSessions = commandSessions.map { case (sessionProducer, session) =>
      if (sessionProducer == clusterProducer) (sessionProducer, session.registered(registeredId))
      else (sessionProducer, session)
    }
    commandSessions(clusterProducer).commands.map(command => SendDownstream(RaftClusterClient(clusterProducer.cluster, registeredId), command))
  }

  /**
    * Filters out commands that have no registered clients and maps registered commands to the registered clientId
    *
    * @param commands sequence of commands for which to resolve the clientId
    * @return commands with registered ids
    */
  def resolveCommands(producer: ClientProducer, commands: Seq[DownstreamCommand]): Seq[SendDownstream] = {
    commands.flatMap(command => resolveCommand(producer, command))
  }

  def registerDownstreamEvents(producer: ClientProducer, downstreamCommands: Seq[DownstreamCommand]): Seq[RegisterDownstreamClient] = {
    downstreamCommands.filter(command => commandSessions.get(commandClusterProducer(producer, command)).isEmpty)
      .map(command => RegisterDownstreamClient(RaftClusterProducer(command.cluster, producer))).distinct
  }

  def registeredDownstreamClientSessions(): Map[RaftClusterClient, DownstreamCommandSession] =
    commandSessions.flatMap { case (clusterProducer, session) =>
      clusterClientForProducer(clusterProducer).map(clusterClient => (clusterClient, session))
    }

  def unregisteredSessionClients(): Seq[RegisterDownstreamClient] = {
    commandSessions.filter(_._2.clientId.isEmpty).keys.map(clusterProducer => RegisterDownstreamClient(clusterProducer)).toSeq
  }

  private def commandClusterProducer(producer: ClientProducer, command: DownstreamCommand): RaftClusterProducer =
    RaftClusterProducer(command.cluster, producer)

  private def clusterClientForProducer(clusterProducer: RaftClusterProducer): Option[RaftClusterClient] =
    commandSessions.get(clusterProducer).flatMap(session => session.clientId).map(clientId => RaftClusterClient(clusterProducer.cluster, clientId))

  private def resolveCommand(producer: ClientProducer, command: DownstreamCommand): Option[SendDownstream] =
    clusterClientForProducer(commandClusterProducer(producer, command)).map(clusterClient => SendDownstream(clusterClient, command.command))
}
