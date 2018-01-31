package rx.distributed.raft.forwarding

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.logging.log4j.scala.Logging
import rx.distributed.raft.client.RaftClusterClient
import rx.distributed.raft.consensus.server.Address.ServerAddress
import rx.distributed.raft.events.Event
import rx.distributed.raft.events.forwarding._
import rx.distributed.raft.events.statemachine._
import rx.distributed.raft.rpc.client.RaftClient
import rx.distributed.raft.statemachine.input.StatemachineCommand
import rx.distributed.raft.statemachine.session.{CommandSessions, DownstreamCommandSession, RaftClusterProducer}
import rx.distributed.raft.util.IndexedResponse
import rx.lang.scala.schedulers.NewThreadScheduler
import rx.lang.scala.subjects.PublishSubject
import rx.lang.scala.{Observable, Subject, Subscription}

import scala.collection.immutable.HashMap

case class ForwardingModule(address: ServerAddress, events: Observable[Event], timeoutSeconds: Int, keepAliveTimeoutSeconds: Int) extends Logging {
  private val leader: AtomicBoolean = new AtomicBoolean(false)
  private val outputSubject: Subject[Event] = PublishSubject().toSerialized
  val observable: Observable[Event] = outputSubject.doOnUnsubscribe(steppedDown())
  var clients: Map[RaftClusterClient, ForwardingClientSource] = HashMap()
  var registeringClients: Map[RaftClusterProducer, Subscription] = HashMap()

  private val inputEvents = events.collect { case forwardingEvent: ForwardingModuleEvent => forwardingEvent }.observeOn(NewThreadScheduler())
  inputEvents.subscribe(handleInputEvent,
    throwable => logger.error(s"[$address] Consensus module input observable threw $throwable"),
    () => logger.info(s"[$address] Consensus module input observable completed."))

  private def handleInputEvent(event: ForwardingModuleEvent): Unit = {
    if(leader.get()) logger.info(s"[$address] Forwarding module received input event: $event")
    event match {
      case BecameLeader(commandSessions) => handleBecameLeader(commandSessions)
      case LeaderSteppedDown() if leader.get() => steppedDown()
      case RegisterDownstreamClient(clusterProducer) if leader.get() => registerDownstreamClient(clusterProducer)
      case SendDownstream(clusterClient, statemachineCommand) if leader.get() => sendCommand(clusterClient, statemachineCommand)
      case RemovedCommands(clusterClient, seqNum) if leader.get() => updateResponseSeqNum(clusterClient, seqNum)
      case DownstreamClientExpired(client) if leader.get() => expireDownstream(client)
      case _ =>
    }
  }

  private def sendCommand(clusterClient: RaftClusterClient, statemachineCommand: StatemachineCommand): Unit = {
    getDownstream(clusterClient).source.onNext(statemachineCommand)
  }

  // TODO: what happens if clients(raftClusterClient) already completed? - will we keep values forever in replaySubject?
  private def getDownstream(raftClusterClient: RaftClusterClient): ForwardingClientSource = {
    if (!clients.contains(raftClusterClient)) {
      val downstream = createDownstream(raftClusterClient, 0)
      clients = clients + (raftClusterClient -> downstream)
    }
    clients(raftClusterClient)
  }

  private def createDownstream(raftClusterClient: RaftClusterClient, startingSequenceNumber: Int): ForwardingClientSource = {
    val source = PublishSubject[StatemachineCommand]()
    val client = RaftClient(raftClusterClient.cluster, timeoutSeconds, keepAliveTimeoutSeconds, Seq())
    val (responses, tracker) = client.sendCommands(raftClusterClient.id, source, startingSequenceNumber, startingSequenceNumber)
    val subscription = responses.subscribe(
      response => handleDownstreamResponse(raftClusterClient, response),
      throwable => {
        logger.error(s"[$address] Forwarding module response from client $raftClusterClient threw: $throwable")
        outputSubject.onError(throwable)
      })
    val downstream = ForwardingClientSource(source, tracker, subscription)
    downstream
  }

  private def registerDownstreamClient(clusterProducer: RaftClusterProducer): Unit = {
    val jarBytes = clusterProducer.producer.jars.map(jar => jar.bytes)
    val client = RaftClient(clusterProducer.cluster, timeoutSeconds, keepAliveTimeoutSeconds, jarBytes)
    val subscription = client.register().take(1).subscribe(clientId => {
      val raftClusterClient = RaftClusterClient(clusterProducer.cluster, clientId)
      val source = PublishSubject[StatemachineCommand]()
      val (responses, tracker) = client.sendCommands(clientId, source)
      val subscription = responses.subscribe(
        response => handleDownstreamResponse(raftClusterClient, response),
        throwable => {
          logger.error(s"[$address] Forwarding module response from client $raftClusterClient threw: $throwable")
          outputSubject.onError(throwable)
        })
      val downstream = ForwardingClientSource(source, tracker, subscription)
      clients = clients + (raftClusterClient -> downstream)
      outputSubject.onNext(RegisteredDownstreamClient(clusterProducer, clientId, System.currentTimeMillis()))
      registeringClients = registeringClients - clusterProducer
    })
    registeringClients = registeringClients + (clusterProducer -> subscription)
  }

  private def handleDownstreamResponse(raftClusterClient: RaftClusterClient, response: IndexedResponse): Unit = {
    outputSubject.onNext(RemoveCommand(raftClusterClient, response.index, System.currentTimeMillis()))
  }

  private def updateResponseSeqNum(clusterClient: RaftClusterClient, seqNum: Int): Unit = {
    clients(clusterClient).responseTracker.addResponse(seqNum)
  }

  private def sendExistingCommands(sessions: Map[RaftClusterClient, DownstreamCommandSession]): Unit = {
    clients = sessions.map { case (clusterClient, session) =>
      val downstream = createDownstream(clusterClient, session.startingSeqNum)
      session.commands.foreach(downstream.source.onNext)
      (clusterClient, downstream)
    }
  }

  private def handleBecameLeader(commandSessions: CommandSessions): Unit = {
    leader.set(true)
    logger.info(s"[$address] Became leader.")
    sendExistingCommands(commandSessions.registeredDownstreamClientSessions())
    commandSessions.unregisteredSessionClients().foreach(registerClientEvent => registerDownstreamClient(registerClientEvent.raftClusterProducer))
  }

  private def steppedDown(): Unit = {
    leader.set(false)
    logger.info(s"[$address] Stepped down from leader.")
    clients.values.foreach(forwardingClientSource => {
      forwardingClientSource.source.onCompleted()
      forwardingClientSource.subscription.unsubscribe()
    })
    registeringClients.foreach(_._2.unsubscribe())
    clients = HashMap()
  }

  private def expireDownstream(raftClusterClient: RaftClusterClient): Unit = {
    clients.get(raftClusterClient).foreach(_.source.onCompleted())
  }
}
