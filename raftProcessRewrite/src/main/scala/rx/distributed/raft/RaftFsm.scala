package rx.distributed.raft

import org.apache.logging.log4j.scala.Logging
import rx.distributed.raft.consensus.ConsensusModule
import rx.distributed.raft.consensus.config.{ClusterConfig, Config}
import rx.distributed.raft.consensus.server.Address.ServerAddress
import rx.distributed.raft.events.Event
import rx.distributed.raft.events.client.ExpireSessions
import rx.distributed.raft.events.rpc.request.ClientRpcRequest
import rx.distributed.raft.forwarding.ForwardingModule
import rx.distributed.raft.rpc.client.ConsensusRpcClient
import rx.distributed.raft.rpc.server.RaftRPCServer
import rx.distributed.raft.statemachine.{Statemachine, StatemachineModule}
import rx.distributed.raft.timeout.RxTimeout
import rx.lang.scala.Subject
import rx.lang.scala.subjects.PublishSubject
import rx.lang.scala.subscriptions.CompositeSubscription

case class RaftFsm(events: Subject[Event], statemachineModule: StatemachineModule, consensusModule: ConsensusModule, rpcServer: RaftRPCServer,
                   consensusRpcClient: ConsensusRpcClient, forwardingModule: ForwardingModule) extends Logging {
  private var subscription = CompositeSubscription()
  private val commandTimeout = RxTimeout(statemachineModule.statemachine.sessionTimeout + 1000)
  subscription += rpcServer.observable.subscribe(events)
  subscription += consensusModule.observable.subscribe(events)
  subscription += statemachineModule.observable.subscribe(events)
  subscription += consensusRpcClient.observable.subscribe(events)
  subscription += forwardingModule.observable.subscribe(events)
  subscription += commandTimeout.map(_ => ExpireSessions(System.currentTimeMillis())).subscribe(event => {
    commandTimeout.stop()
    events.onNext(event)
  })

  events.collect { case clientRequest: ClientRpcRequest => clientRequest }.subscribe(_ => commandTimeout.reset())
  consensusModule.electionTimeoutSource.start()

  def shutdown(): Unit = {
    events.onCompleted()
    subscription.unsubscribe()
  }

}

object RaftFsm {
  def apply(address: ServerAddress, config: Config, clusterConfig: ClusterConfig, statemachine: Statemachine): RaftFsm = {
    val events = PublishSubject[Event].toSerialized
    val rpcServer = RaftRPCServer(address)
    val consensusModule = ConsensusModule(address, config, clusterConfig, events)
    val statemachineModule = StatemachineModule(address, statemachine, events)
    val consensusRpcClient = ConsensusRpcClient(address, events)
    val forwardingModule = ForwardingModule(address, events, 60, 1)
    RaftFsm(events, statemachineModule, consensusModule, rpcServer, consensusRpcClient, forwardingModule)
  }

  def synchronous(address: ServerAddress, config: Config, clusterConfig: ClusterConfig, statemachine: Statemachine): RaftFsm = {
    val events = PublishSubject[Event].toSerialized
    val rpcServer = RaftRPCServer(address)
    val consensusModule = ConsensusModule.synchronous(address, config, clusterConfig, events)
    val statemachineModule = StatemachineModule(address, statemachine, events)
    val consensusRpcClient = ConsensusRpcClient(address, events)
    val forwardingModule = ForwardingModule(address, events, 60, 1)
    RaftFsm(events, statemachineModule, consensusModule, rpcServer, consensusRpcClient, forwardingModule)
  }
}
