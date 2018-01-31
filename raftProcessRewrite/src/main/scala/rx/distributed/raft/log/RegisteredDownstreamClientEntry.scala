package rx.distributed.raft.log

import rx.distributed.raft.LogEntryProto
import rx.distributed.raft.statemachine.session.RaftClusterProducer
import rx.distributed.raft.util.ByteStringSerializer

case class RegisteredDownstreamClientEntry(term: Int, clusterProducer: RaftClusterProducer, registeredId: String, timestamp: Long) extends LogEntry {

  override def toProtobuf: LogEntryProto = {
    LogEntryProto.newBuilder()
      .setType(LogEntryProto.EntryType.REGISTER_DOWNSTREAM_CLIENT)
      .setTerm(term)
      .setClientId(registeredId)
      .setTimestamp(timestamp)
      .setCommand(ByteStringSerializer.write(clusterProducer))
      .build()
  }
}
