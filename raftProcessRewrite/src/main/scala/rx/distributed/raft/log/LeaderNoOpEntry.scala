package rx.distributed.raft.log

import rx.distributed.raft._

case class LeaderNoOpEntry(term: Int, timestamp: Long) extends LogEntry {
  override def toProtobuf: LogEntryProto = {
    LogEntryProto.newBuilder()
      .setType(LogEntryProto.EntryType.NO_OP)
      .setTerm(term)
      .setTimestamp(timestamp)
      .build()
  }
}
