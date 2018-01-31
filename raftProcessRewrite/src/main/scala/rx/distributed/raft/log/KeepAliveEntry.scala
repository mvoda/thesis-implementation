package rx.distributed.raft.log

import rx.distributed.raft._
import rx.distributed.raft.events.client.KeepAliveEvent

case class KeepAliveEntry(term: Int, event: KeepAliveEvent) extends LogEntry {
  override def timestamp: Long = event.timestamp

  override def toProtobuf: LogEntryProto = {
    LogEntryProto.newBuilder()
      .setType(LogEntryProto.EntryType.KEEP_ALIVE)
      .setTerm(term)
      .setClientId(event.clientId)
      .setResponseSequenceNum(event.responseSeqNum)
      .setTimestamp(event.timestamp)
      .build()
  }
}
