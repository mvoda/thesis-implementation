package rx.distributed.raft.log

import java.util.NoSuchElementException

import rx.distributed.raft.consensus.config.ClusterConfig
import rx.distributed.raft.events.consensus.AppendEntries

import scala.util.{Success, Try}

case class RaftLog private(private val log: Seq[LogEntry] = List(null), private val bootstrapCluster: ClusterConfig) extends Serializable {

  def lastIndex: Int = log.size - 1

  def lastTerm: Int = if (lastIndex != 0) log.last.term else -1

  // checks if log is consistent up until prevLogIndex and prevLogTerm
  def isConsistent(prevLogIndex: Int, prevLogTerm: Int): Boolean = {
    if (prevLogIndex == 0) {
      true
    } else {
      Try(log(prevLogIndex)) match {
        case Success(logEntry) if logEntry.term == prevLogTerm => true
        case _ => false
      }
    }
  }

  // TODO: prevent changing entries that have been committed to the state machine
  // AppendEntries rules:
  //    - if an existing entry conflicts with a new one (same index, different terms)
  //      then delete existing entry and all that follow it and append any new entries not already in the log
  //    - if a follower receives an AppendEntries request that includes log entries already present in its log,
  //      it ignores those entries in the new request.
  def appendEntries(entries: Seq[LogEntry], prevLogIndex: Int): RaftLog = {
    val matchingEntries = log.drop(prevLogIndex + 1).zip(entries).takeWhile(x => x._1.term == x._2.term).size
    if (matchingEntries == entries.size) this
    else
      new RaftLog(log.take(1 + prevLogIndex + matchingEntries) ++ entries.drop(matchingEntries), bootstrapCluster)
  }

  def appendEntries(event: AppendEntries): RaftLog = {
    if (isConsistent(event.prevLogIndex, event.prevLogTerm)) {
      return appendEntries(event.entries, event.prevLogIndex)
    }
    this
  }

  def :+(newEntry: LogEntry): RaftLog = new RaftLog(log :+ newEntry, bootstrapCluster)

  lazy val clusterConfig: ClusterConfig = latestCluster(log)

  // get the latest cluster from the log (update when reimplementing membership changes)
  private def latestCluster(log: Seq[LogEntry]): ClusterConfig = bootstrapCluster

  def isMoreUpToDate(otherLastLogIndex: Int, otherLastLogTerm: Int): Boolean = {
    //  - compare index and term of last entries in logs
    //  - if logs have entries with different terms then log with later term is more up to date
    //  - if logs end with same term then the longer log is more up to date
    if (lastIndex != 0 && lastTerm != otherLastLogTerm) {
      // log is not empty & terms are different
      return lastTerm > otherLastLogTerm
    }
    lastIndex > otherLastLogIndex
  }

  // entries between first (exclusive) and last (inclusive)
  def getEntriesBetween(first: Int, last: Int): Seq[LogEntry] = log.slice(first + 1, last + 1)

  def getEntriesAfter(index: Int): Seq[LogEntry] = log.drop(index + 1)

  def allEntries(): Seq[LogEntry] = getEntriesAfter(0)

  def apply(index: Int): LogEntry = if (index == 0) throw new NoSuchElementException else log(index)

  def getTerm(index: Int): Int = if (index == 0) -1 else log(index).term

  def size: Int = log.size - 1

  override def toString: String = log.drop(1).toString
}

object RaftLog {
  def create(entries: Seq[LogEntry], clusterConfig: ClusterConfig): RaftLog = {
    RaftLog(List(null) ++ entries, clusterConfig)
  }

  def apply(clusterConfig: ClusterConfig): RaftLog = {
    RaftLog(List(null), clusterConfig)
  }
}
