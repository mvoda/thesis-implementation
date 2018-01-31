package rx.distributed.raft.consensus.state

import org.apache.logging.log4j.scala.Logging
import rx.distributed.raft.PersistentStateProto
import rx.distributed.raft.consensus.PersistentStateIO
import rx.distributed.raft.consensus.config.ClusterConfig
import rx.distributed.raft.consensus.server.Address
import rx.distributed.raft.consensus.server.Address.ServerAddress
import rx.distributed.raft.converters.ProtobufConverters
import rx.distributed.raft.log.{LeaderNoOpEntry, LogEntry, RaftLog}

import scala.collection.JavaConverters._
import scala.util.Try

/** Persistent State on all servers - update on stable storage before responding to RPC
  *
  * @param currentTerm latest term server has seen (init to 0 on first boot, increase monotonically)
  * @param votedFor    candidateId that received vote in current term
  * @param log         log entries; each entry contains a command and term when entry was received by leader (first index is 1)
  * @param leaderHint  address of last known leader
  */
case class PersistentState(currentTerm: Int = 0, votedFor: Option[ServerAddress] = None, log: RaftLog, leaderHint: Option[ServerAddress] = None) extends Logging {
  def increaseTerm(): PersistentState = updateTerm(currentTerm + 1)

  def updateTerm(term: Int): PersistentState = term match {
    case newTerm if newTerm <= currentTerm => logger.warn("tried to update persistentState term with value <= current term")
      this
    case newTerm if newTerm > currentTerm => PersistentState(newTerm, None, log, None)
  }

  def updateLog(newLog: RaftLog): PersistentState = {
    if (newLog != log) PersistentState(currentTerm, votedFor, newLog, leaderHint)
    else this
  }

  def appendEntry(entry: LogEntry): PersistentState = {
    PersistentState(currentTerm, votedFor, log :+ entry)
  }

  def appendNoOp(): PersistentState = appendEntry(LeaderNoOpEntry(currentTerm, System.currentTimeMillis()))

  def appendEntry(entry: LogEntry, prevLogIndex: Int): PersistentState =
    PersistentState(currentTerm, votedFor, log.appendEntries(Seq(entry), prevLogIndex), leaderHint)

  def appendEntries(entries: Seq[LogEntry], prevLogIndex: Int): PersistentState =
    PersistentState(currentTerm, votedFor, log.appendEntries(entries, prevLogIndex), leaderHint)

  def voteFor(address: ServerAddress): PersistentState = votedFor match {
    case None => PersistentState(currentTerm, Some(address), log, leaderHint)
    case Some(currentVote) if currentVote != address =>
      logger.warn(s"tried to vote twice. current vote: $currentVote; new vote: $address")
      this
    case _ => this
  }

  def updateLeaderHint(newLeaderHint: Option[ServerAddress]): PersistentState = {
    if (newLeaderHint != leaderHint) PersistentState(currentTerm, votedFor, log, newLeaderHint)
    else this
  }

  def updateLeaderHint(newLeaderHint: ServerAddress): PersistentState = updateLeaderHint(Some(newLeaderHint))

  def toProtobuf(): PersistentStateProto = {
    val builder = PersistentStateProto.newBuilder().setTerm(currentTerm).addAllLogEntries(log.allEntries().map(_.toProtobuf).asJava)
    votedFor.foreach(address => builder.setVotedFor(address.toProtobuf))
    leaderHint.foreach(address => builder.setLeaderHint(address.toProtobuf))
    builder.build()
  }
}

object PersistentState extends Logging {
  private def default(address: ServerAddress, bootstrapLog: RaftLog): PersistentState = {
    logger.info(s"[$address] Creating default persistent state")
    PersistentState(0, None, bootstrapLog)
  }

  def apply(persistentStateIO: PersistentStateIO, bootstrapLog: RaftLog): PersistentState = {
    Try(persistentStateIO.read(bootstrapLog.clusterConfig)).getOrElse(default(persistentStateIO.address, bootstrapLog))
  }

  def fromProtobuf(proto: PersistentStateProto, clusterConfig: ClusterConfig): PersistentState = {
    val votedFor = Option(proto.hasVotedFor).collect { case true => proto.getVotedFor }.map(addressProto => Address(addressProto))
    val leaderHint = Option(proto.hasLeaderHint).collect { case true => proto.getLeaderHint }.map(addressProto => Address(addressProto))
    val logEntries = ProtobufConverters.fromProtobuf(proto.getLogEntriesList)
    PersistentState(proto.getTerm, votedFor, RaftLog.create(logEntries, clusterConfig), leaderHint)
  }

}
