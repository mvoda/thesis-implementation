package rx.distributed.raft.log

import rx.distributed.raft._

trait LogEntry {
  def term: Int

  def timestamp: Long

  def toProtobuf: LogEntryProto
}
