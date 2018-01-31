package rx.distributed.raft.events.statemachine

import rx.distributed.raft.client.RaftClusterClient
import rx.distributed.raft.events.Event
import rx.distributed.raft.events.forwarding.ForwardingModuleEvent
import rx.distributed.raft.statemachine.input.StatemachineCommand
import rx.distributed.raft.statemachine.session.{CommandSessions, RaftClusterProducer}

sealed trait StatemachineOutputEvent extends Event

case class DownstreamClientExpired(client: RaftClusterClient) extends StatemachineOutputEvent with ForwardingModuleEvent

case class SendDownstream(raftClusterClient: RaftClusterClient, command: StatemachineCommand) extends StatemachineOutputEvent with ForwardingModuleEvent

case class RemovedCommands(clusterClient: RaftClusterClient, commandSeqNum: Int) extends StatemachineOutputEvent with ForwardingModuleEvent

case class RegisterDownstreamClient(raftClusterProducer: RaftClusterProducer) extends StatemachineOutputEvent with ForwardingModuleEvent

case class BecameLeader(commandSessions: CommandSessions) extends StatemachineOutputEvent with ForwardingModuleEvent
