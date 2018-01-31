package rx.distributed.raft.log

import rx.distributed.raft._
import rx.distributed.raft.client.RaftClusterClient

import scala.collection.JavaConverters._

case class RemoveCommandsEntry(term: Int, downstreamClient: RaftClusterClient, seqNum: Int, timestamp: Long) extends LogEntry {
  override def toProtobuf: LogEntryProto = {
    LogEntryProto.newBuilder()
      .setType(LogEntryProto.EntryType.REMOVE_COMMANDS)
      .setTerm(term)
      .setClientId(downstreamClient.id)
      .setTimestamp(timestamp)
      .setSequenceNum(seqNum)
      .addAllServers(downstreamClient.cluster.map(_.toProtobuf).asJava)
      .build()
  }
}
