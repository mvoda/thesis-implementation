package rx.distributed.raft.log

import org.junit.Test
import org.scalatest.MustMatchers._
import rx.distributed.raft.Utils
import rx.distributed.raft.Utils.clientLogEntry
import rx.distributed.raft.consensus.config.ClusterConfig

class RaftLogTest {
  val config: ClusterConfig = Utils.testCluster()
  var log: RaftLog = RaftLog(config)

  @Test
  def initialLogIsEmpty(): Unit = {
    log.lastIndex mustBe 0
  }

  @Test
  def firstIndexIsOne(): Unit = {
    val newLog = log :+ clientLogEntry(0)
    newLog.lastIndex mustBe 1
  }

  @Test
  def emptyLogsAreConsistent(): Unit = {
    log.isConsistent(0, 0) mustBe true
  }

  @Test
  def emptyLogIsNotMoreUpToDateThanEmpty(): Unit = {
    log.isMoreUpToDate(0, 0) mustBe false
  }

  @Test
  def logIsMoreUpToDateThanEmpty(): Unit = {
    val newLog = log :+ clientLogEntry(0)
    newLog.isMoreUpToDate(0, 0) mustBe true
  }

  @Test
  def logHigherTermIsMoreUpToDate(): Unit = {
    val newLog = log :+ clientLogEntry(5)
    newLog.isMoreUpToDate(5, 1) mustBe true
  }

  @Test
  def longerLogIsMoreUpToDate(): Unit = {
    val newLog = log :+ clientLogEntry(1)
    newLog.isMoreUpToDate(5, 1) mustBe false
  }

  @Test
  def testGetEntriesAfter0(): Unit = {
    val newLog = log :+ clientLogEntry(1) :+ clientLogEntry(2)
    newLog.getEntriesAfter(0).size mustBe 2
  }

  @Test
  def testGetEntriesAfter1(): Unit = {
    val newLog = log :+ clientLogEntry(1) :+ clientLogEntry(2)
    newLog.getEntriesAfter(1).size mustBe 1
    newLog.getEntriesAfter(1).head.term mustBe 2
  }

  @Test
  def testGetEntriesBetween02(): Unit = {
    val newLog = log :+ clientLogEntry(1) :+ clientLogEntry(2)
    val entries = newLog.getEntriesBetween(0, 2)
    entries.size mustBe 2
    entries.head.term mustBe 1
    entries.tail.head.term mustBe 2
  }

  @Test
  def testGetEntriesBetween12(): Unit = {
    val newLog = log :+ clientLogEntry(1) :+ clientLogEntry(2)
    val entries = newLog.getEntriesBetween(1, 2)
    entries.size mustBe 1
    entries.head.term mustBe 2
  }

  @Test
  def testGetEntriesBetween22(): Unit = {
    val newLog = log :+ clientLogEntry(1) :+ clientLogEntry(2)
    val entries = newLog.getEntriesBetween(2, 2)
    entries.size mustBe 0
  }

  @Test
  def backwardsConsistency(): Unit = {
    val newLog = log :+ clientLogEntry(1) :+ clientLogEntry(2)
    newLog.isConsistent(2, 2) mustBe true
    newLog.isConsistent(1, 1) mustBe true
    newLog.isConsistent(0, 0) mustBe true
  }

  @Test
  def appendEntriesAdd(): Unit = {
    val newLog = log.appendEntries(Seq(clientLogEntry(1), clientLogEntry(1), clientLogEntry(2), clientLogEntry(3)), 0)
    newLog.size mustBe 4
    newLog.getEntriesAfter(0).map(_.term) mustBe List(1, 1, 2, 3)
    newLog(1).term mustBe 1
    newLog(2).term mustBe 1
    newLog(3).term mustBe 2
    newLog(4).term mustBe 3
  }

  @Test
  def appendEntriesRewrite(): Unit = {
    // terms: (1, 1, 2, 2, 2)
    val log1 = log :+ clientLogEntry(1) :+ clientLogEntry(1) :+ clientLogEntry(2) :+ clientLogEntry(2) :+ clientLogEntry(2)
    log1.size mustBe 5
    val log2 = log1.appendEntries(Seq(clientLogEntry(2)), 0)
    log2.size mustBe 1
    log2(1).term mustBe 2
  }

  @Test
  def appendEntriesPartialRewrite(): Unit = {
    val log1 = log :+ clientLogEntry(1) :+ clientLogEntry(1) :+ clientLogEntry(2) :+ clientLogEntry(2) :+ clientLogEntry(2)
    val log2 = log1.appendEntries(Seq(clientLogEntry(2), clientLogEntry(5), clientLogEntry(9)), 2)
    log2.size mustBe 5
    log2(3).term mustBe 2
    log2(4).term mustBe 5
    log2(5).term mustBe 9
  }
}
