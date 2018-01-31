package rx.distributed.raft.consensus

import org.apache.logging.log4j.scala.Logging
import rx.distributed.raft.consensus.config.{ClusterConfig, Config}
import rx.distributed.raft.consensus.server.Address.ServerAddress
import rx.distributed.raft.consensus.state.ConsensusState
import rx.distributed.raft.consensus.state.RaftState.Leader
import rx.distributed.raft.consensus.transitions.StateTransitions
import rx.distributed.raft.events.Event
import rx.distributed.raft.events.client.{ClientCommandEvent, ClientEvent, KeepAliveEvent, RegisterClientEvent}
import rx.distributed.raft.events.consensus._
import rx.distributed.raft.events.forwarding.LeaderSteppedDown
import rx.distributed.raft.events.internal._
import rx.distributed.raft.events.rpc.request.{AppendEntriesRpcRequest, ConsensusRpcRequest, VoteRequestRpcRequest}
import rx.distributed.raft.events.rpc.response._
import rx.distributed.raft.events.statemachine.CommitLogEntry
import rx.distributed.raft.log.RaftLog
import rx.distributed.raft.timeout.RxTimeout
import rx.lang.scala.schedulers.{ImmediateScheduler, NewThreadScheduler, TestScheduler}
import rx.lang.scala.subjects.PublishSubject
import rx.lang.scala.{Observable, Observer, Scheduler, Subject, Subscription}

case class ConsensusModule(address: ServerAddress, config: Config, clusterConfig: ClusterConfig, events: Observable[Event],
                           electionTimeoutSource: RxTimeout, heartbeatTimeoutSource: RxTimeout, scheduler: Scheduler,
                           persistentStateIO: PersistentStateIO) extends Logging {
  private val feedbackSubject: Subject[Event] = PublishSubject()
  private val outputSubject: Subject[Event] = PublishSubject()
  private var state = ConsensusState(persistentStateIO, config, RaftLog(clusterConfig))
  val observable: Observable[Event] = outputSubject

  private val inputEvents = events.doOnCompleted {
    feedbackSubject.onCompleted()
    electionTimeoutSource.onCompleted()
    heartbeatTimeoutSource.onCompleted()
  }.merge(feedbackSubject).collect {
    case request: ConsensusRpcRequest => request
    case response: ConsensusRpcResponse => response
    case clientEvent: ClientEvent => clientEvent
  }.merge(electionTimeoutSource.map(_ => ElectionTimeout()))
    .merge(heartbeatTimeoutSource.map(_ => HeartbeatTimeout()))
    .observeOn(scheduler)

  inputEvents.subscribe(handleInputEvent,
    throwable => logger.error(s"[${state.address}] Consensus module input observable threw $throwable"),
    () => logger.info(s"[${state.address}] Consensus module input observable completed."))

  def handleInputEvent(inputEvent: Event): Unit = {
//    logger.info(s"[${state.address}] Consensus module received input event: $inputEvent")
    val (nextState, outputEvents) = inputEvent match {
      case consensusRequest: ConsensusRpcRequest => StateTransitions.applyEvent(state, consensusRequest.event)
      case consensusResponse: ConsensusRpcResponse => StateTransitions.applyEvent(state, consensusResponse)
      case clientEvent: ClientEvent => StateTransitions.applyEvent(state, clientEvent)
      case heartbeatTimeout: HeartbeatTimeout => StateTransitions.applyEvent(state, heartbeatTimeout)
      case electionTimeout: ElectionTimeout => StateTransitions.applyEvent(state, electionTimeout)

    }
    if (state.state.isInstanceOf[Leader] && !nextState.state.isInstanceOf[Leader]) {
      outputSubject.onNext(LeaderSteppedDown())
    }
    state = nextState
    val observer = activeLeaderObserver(inputEvent)
    outputEvents.foreach(handleOutputEvent(inputEvent, nextState, observer))
  }

  private def handleOutputEvent(inputEvent: Event, state: ConsensusState, activeLeaderObserver: Option[Observer[AppendEntriesResponse]])
                               (outputEvent: InternalEvent): Unit = outputEvent match {
    case ResetElectionTimeout() => electionTimeoutSource.reset()
    case ResetHeartbeatTimeout() => heartbeatTimeoutSource.reset()
    case StopHeartbeatTimeout() => heartbeatTimeoutSource.stop()
    case CommitEntries(start, end) =>
      state.log.getEntriesBetween(start, end).zipWithIndex.map { case (entry, index) => CommitLogEntry(entry, index + start + 1) }.foreach(outputSubject.onNext)
    case SelfVoteGranted(voteGranted) => feedbackSubject.onNext(voteGranted)
    case SelfAppendEntriesSuccess(appendEntriesSuccess) =>
      feedbackSubject.onNext(appendEntriesSuccess)
      activeLeaderObserver.foreach(observer => observer.onNext(appendEntriesSuccess))
    case requestVotes: RequestVotesFromServer => outputSubject.onNext(requestVotes)
    case appendEntries: AppendEntriesToServer => outputSubject.onNext(CountingAppendEntriesToServer(appendEntries, activeLeaderObserver))
    case response: Response => sendConsensusResponse(inputEvent, response, state)
    case NotLeader() => sendNotLeaderResponse(inputEvent, state.leaderHint)
    case SyncToDiskAndThen(persistentState, nextEvent) =>
      persistentStateIO.write(persistentState).subscribe(_ => handleOutputEvent(inputEvent, state, activeLeaderObserver)(nextEvent), e => throw e)
  }

  private def sendConsensusResponse(event: Event, response: Response, consensusState: ConsensusState): Unit = event match {
    case VoteRequestRpcRequest(_, observer) =>
      Observable.just(VoteResponse.apply(response, consensusState.currentTerm, consensusState.address)).subscribe(observer)
    case AppendEntriesRpcRequest(_, observer) =>
      Observable.just(AppendEntriesResponse.apply(response, consensusState.currentTerm, consensusState.address, consensusState.log.lastIndex)).subscribe(observer)
    case _ => logger.warn(s"State transitions produced response($response), but event ($event) was not a request!")
  }

  private def sendNotLeaderResponse(event: Event, leaderHint: Option[ServerAddress]): Unit = event match {
    case registerClient: RegisterClientEvent => Observable.just(RegisterClientNotLeader(leaderHint)).subscribe(registerClient.observer)
    case clientCommand: ClientCommandEvent => Observable.just(ClientResponseNotLeader(leaderHint)).subscribe(clientCommand.observer)
    case keepAlive: KeepAliveEvent => Observable.just(KeepAliveNotLeader(leaderHint)).subscribe(keepAlive.observer)
    case _ => logger.warn(s"State transitions produced NotLeader response, but event ($event) was not a client request!")
  }

  private def activeLeaderObserver(inputEvent: Event): Option[Observer[AppendEntriesResponse]] = inputEvent match {
    case _: ClientEvent | _: HeartbeatTimeout =>
      val subscription = timeoutSubscription(inputEvent)
      val responseSubject = PublishSubject[AppendEntriesResponse]()
      responseSubject.serialize.filter(_.isInstanceOf[AppendEntriesSuccess]).take(clusterConfig.votingServers.size / 2 + 1)
        .subscribe(_ => (), e => throw e, () => {
          subscription.foreach(_.unsubscribe())
          electionTimeoutSource.reset()
          responseSubject.onCompleted()
        })
      Some(responseSubject)
    case _ => None
  }

  private def timeoutSubscription(inputEvent: Event): Option[Subscription] = inputEvent match {
    case _: ClientEvent => Some(electionTimeoutSource.take(1).subscribe(_ => sendNotLeaderResponse(inputEvent, None)))
    case _ => None
  }

  def consensusState: ConsensusState = state

  def cleanupState(): Unit = persistentStateIO.cleanup()
}

object ConsensusModule {
  def apply(address: ServerAddress, config: Config, clusterConfig: ClusterConfig, events: Observable[Event]): ConsensusModule = {
    ConsensusModule(address, config, clusterConfig, events, RxTimeout(config), RxTimeout(config.heartbeatTimeout), NewThreadScheduler(), PersistentStateIO(address))
  }

  def apply(address: ServerAddress, config: Config, clusterConfig: ClusterConfig, events: Observable[Event], persistentStateIO: PersistentStateIO): ConsensusModule = {
    ConsensusModule(address, config, clusterConfig, events, RxTimeout(config), RxTimeout(config.heartbeatTimeout), NewThreadScheduler(), persistentStateIO)
  }

  def synchronous(address: ServerAddress, config: Config, clusterConfig: ClusterConfig, events: Observable[Event]): ConsensusModule = {
    ConsensusModule(address, config, clusterConfig, events, RxTimeout(config, TestScheduler()),
      RxTimeout(config.heartbeatTimeout, TestScheduler()), ImmediateScheduler(), PersistentStateIO(address, ImmediateScheduler()))
  }
}