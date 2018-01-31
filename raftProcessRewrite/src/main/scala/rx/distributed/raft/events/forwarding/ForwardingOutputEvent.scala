package rx.distributed.raft.events.forwarding

import rx.distributed.raft.client.RaftClusterClient
import rx.distributed.raft.consensus.state.PersistentState
import rx.distributed.raft.events.Event
import rx.distributed.raft.events.client.ClientEvent
import rx.distributed.raft.log.{LogEntry, RegisteredDownstreamClientEntry, RemoveCommandsEntry}
import rx.distributed.raft.statemachine.session.RaftClusterProducer

trait ForwardingOutputEvent extends Event

case class RemoveCommand(clusterClient: RaftClusterClient, seqNum: Int, timestamp: Long) extends ForwardingOutputEvent with ClientEvent {
  def toLogEntry(persistentState: PersistentState): LogEntry = RemoveCommandsEntry(persistentState.currentTerm, clusterClient, seqNum, timestamp)
}

case class RegisteredDownstreamClient(clusterProducer: RaftClusterProducer, registeredId: String, timestamp: Long) extends ForwardingOutputEvent with ClientEvent {
  def toLogEntry(persistentState: PersistentState): LogEntry =
    RegisteredDownstreamClientEntry(persistentState.currentTerm, clusterProducer, registeredId, timestamp)
}
