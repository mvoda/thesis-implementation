package rx.distributed.raft.converters

import rx.distributed.raft._
import rx.distributed.raft.client.RaftClusterClient
import rx.distributed.raft.consensus.server.Address
import rx.distributed.raft.events.client.{ClientCommandEvent, KeepAliveEvent, RegisterClientEvent}
import rx.distributed.raft.events.consensus.{AppendEntries, VoteRequest}
import rx.distributed.raft.events.rpc.response.ClientCommandResponse
import rx.distributed.raft.log._
import rx.distributed.raft.statemachine.session.RaftClusterProducer
import rx.distributed.raft.util.ByteStringDeserializer
import rx.lang.scala.Observer

import scala.collection.JavaConverters._

object ProtobufConverters {
  def fromProtobuf(request: AppendEntriesRequestProto): AppendEntries =
    AppendEntries(request.getTerm, Address(request.getLeaderAddress), request.getPrevLogIndex,
      request.getPrevLogTerm, fromProtobuf(request.getEntriesList), request.getLeaderCommit)

  def fromProtobuf(logEntries: java.lang.Iterable[LogEntryProto]): Seq[LogEntry] =
    logEntries.asScala.map(fromProtobuf).toSeq

  def fromProtobuf(request: VoteRequestProto): VoteRequest =
    VoteRequest(request.getTerm, Address(request.getCandidateAddress), request.getLastLogIndex, request.getLastLogTerm)

  def fromProtobuf(entry: LogEntryProto): LogEntry = {
    entry.getType match {
      case LogEntryProto.EntryType.REGISTER_CLIENT => RegisterClientEntry(entry.getTerm, entry.getClientId,
        RegisterClientEvent(entry.getJarsList.asScala, entry.getTimestamp)(Observer()))
      case LogEntryProto.EntryType.CLIENT_COMMAND => ClientLogEntry(entry.getTerm,
        ClientCommandEvent(entry.getClientId, entry.getSequenceNum, entry.getResponseSequenceNum, entry.getTimestamp, entry.getCommand)(Observer()))
      case LogEntryProto.EntryType.NO_OP => NoOpEntry(entry.getTerm, entry.getTimestamp)
      case LogEntryProto.EntryType.KEEP_ALIVE => KeepAliveEntry(entry.getTerm,
        KeepAliveEvent(entry.getClientId, entry.getResponseSequenceNum, entry.getTimestamp)(Observer()))
      case LogEntryProto.EntryType.REMOVE_COMMANDS =>
        val clusterClient = RaftClusterClient(entry.getServersList.asScala.map(Address(_)).toSet, entry.getClientId)
        RemoveCommandsEntry(entry.getTerm, clusterClient, entry.getSequenceNum, entry.getTimestamp)
      case LogEntryProto.EntryType.EXPIRE_SESSIONS => ExpireSessionsEntry(entry.getTerm, entry.getTimestamp)
      case LogEntryProto.EntryType.REGISTER_DOWNSTREAM_CLIENT =>
        val clusterProducer = ByteStringDeserializer.deserialize[RaftClusterProducer](entry.getCommand)
        RegisteredDownstreamClientEntry(entry.getTerm, clusterProducer, entry.getClientId, entry.getTimestamp)
      case _ => throw new UnrecognizedLogEntryException(entry)
    }
  }

  def fromProtobuf(request: ClientRequestProto, observer: Observer[ClientCommandResponse]): ClientCommandEvent =
    ClientCommandEvent(request.getClientId, request.getSequenceNum, request.getResponseSequenceNum,
      System.currentTimeMillis(), request.getCommand)(observer)

  class UnrecognizedLogEntryException(entry: LogEntryProto) extends Exception("Could not deserialize log entry: " + entry)

}
