package rx.distributed.raft.events.statemachine

import rx.distributed.raft.events.Event
import rx.distributed.raft.log.LogEntry

case class CommitLogEntry(entry: LogEntry, index: Int) extends Event
