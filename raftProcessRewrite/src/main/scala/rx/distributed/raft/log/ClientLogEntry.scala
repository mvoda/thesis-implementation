package rx.distributed.raft.log

import rx.distributed.raft.LogEntryProto
import rx.distributed.raft.events.client.ClientCommandEvent

case class ClientLogEntry(term: Int, event: ClientCommandEvent) extends LogEntry {
  override def timestamp: Long = event.timestamp

  override def toProtobuf: LogEntryProto = {
    LogEntryProto.newBuilder()
      .setType(LogEntryProto.EntryType.CLIENT_COMMAND)
      .setTerm(term)
      .setClientId(event.clientId)
      .setTimestamp(event.timestamp)
      .setSequenceNum(event.seqNum)
      .setResponseSequenceNum(event.responseSeqNum)
      .setCommand(event.command)
      .build()
  }
}
