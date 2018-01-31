package rx.distributed.raft.events.consensus

import rx.distributed.raft.consensus.server.Address.ServerAddress
import rx.distributed.raft.log.LogEntry
import rx.distributed.raft.{AppendEntriesRequestProto, VoteRequestProto}

import scala.collection.JavaConverters._

sealed trait ConsensusRequest extends ConsensusEvent

final case class AppendEntries(term: Int, address: ServerAddress, prevLogIndex: Int, prevLogTerm: Int, entries: Seq[LogEntry],
                               leaderCommit: Int, receivedTimestamp: Long = System.currentTimeMillis()) extends ConsensusRequest {
  def toProtobuf: AppendEntriesRequestProto =
    AppendEntriesRequestProto.newBuilder().setTerm(term).setLeaderAddress(address.toProtobuf).setPrevLogIndex(prevLogIndex)
      .setPrevLogTerm(prevLogTerm).addAllEntries(entries.map(_.toProtobuf).asJava).setLeaderCommit(leaderCommit).build()
}

final case class VoteRequest(term: Int, address: ServerAddress, lastLogIndex: Int, lastLogTerm: Int,
                             receivedTimestamp: Long = System.currentTimeMillis()) extends ConsensusRequest {
  def toProtobuf: VoteRequestProto =
    VoteRequestProto.newBuilder().setTerm(term).setCandidateAddress(address.toProtobuf).setLastLogIndex(lastLogIndex)
      .setLastLogTerm(lastLogTerm).build()
}
