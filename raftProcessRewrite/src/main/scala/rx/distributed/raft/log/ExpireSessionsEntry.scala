package rx.distributed.raft.log

import rx.distributed.raft._

case class ExpireSessionsEntry(term: Int, timestamp: Long) extends LogEntry {
  override def toProtobuf: LogEntryProto = {
    LogEntryProto.newBuilder()
      .setType(LogEntryProto.EntryType.EXPIRE_SESSIONS)
      .setTerm(term)
      .setTimestamp(timestamp)
      .build()
  }
}
