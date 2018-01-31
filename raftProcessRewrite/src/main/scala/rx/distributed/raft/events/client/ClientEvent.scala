package rx.distributed.raft.events.client

import rx.distributed.raft.consensus.state.PersistentState
import rx.distributed.raft.events.Event
import rx.distributed.raft.log.LogEntry

//TODO: rename to logEvent?
trait ClientEvent extends Event {
  def toLogEntry(persistentState: PersistentState): LogEntry
}
