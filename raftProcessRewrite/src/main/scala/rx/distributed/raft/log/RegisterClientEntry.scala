package rx.distributed.raft.log

import rx.distributed.raft._
import rx.distributed.raft.events.client.RegisterClientEvent

import scala.collection.JavaConverters._

case class RegisterClientEntry(term: Int, clientId: String, event: RegisterClientEvent) extends LogEntry {
  override def timestamp: Long = event.timestamp

  override def toProtobuf: LogEntryProto = {
    LogEntryProto.newBuilder()
      .setType(LogEntryProto.EntryType.REGISTER_CLIENT)
      .setTerm(term)
      .setClientId(clientId)
      .setTimestamp(event.timestamp)
      .addAllJars(event.jarsBytes.asJava)
      .build()
  }
}
