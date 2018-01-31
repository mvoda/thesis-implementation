package rx.distributed.raft.events.client

import rx.distributed.raft.consensus.state.PersistentState
import rx.distributed.raft.log.{ExpireSessionsEntry, LogEntry}

case class ExpireSessions(timestamp: Long) extends ClientEvent {
  def toLogEntry(persistentState: PersistentState): LogEntry = ExpireSessionsEntry(persistentState.currentTerm, timestamp)
}
