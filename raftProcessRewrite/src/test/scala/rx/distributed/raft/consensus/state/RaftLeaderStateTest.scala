package rx.distributed.raft.consensus.state

import org.junit.Test
import org.scalatest.MustMatchers._
import rx.distributed.raft.Utils
import rx.distributed.raft.consensus.config.ClusterConfig
import rx.distributed.raft.consensus.server.Address.TestingAddress

class RaftLeaderStateTest {
  private val SERVER1 = TestingAddress("test_server_1")
  private val SERVER2 = TestingAddress("test_server_2")
  private val SERVER3 = TestingAddress("test_server_3")

  private val config: ClusterConfig = Utils.testCluster()
  private val leaderState: LeaderState = Utils.defaultLeaderState()

  @Test
  def initialNextIndexesAreOne(): Unit = {
    leaderState.nextIndex(SERVER1) mustBe 1
    leaderState.nextIndex(SERVER2) mustBe 1
    leaderState.nextIndex(SERVER3) mustBe 1
  }

  @Test
  def initialMatchIndexesAreZero(): Unit = {
    leaderState.matchIndex(SERVER1) mustBe 0
    leaderState.matchIndex(SERVER2) mustBe 0
    leaderState.matchIndex(SERVER3) mustBe 0
  }

  @Test
  def initialHighestReplicatedIndexIsZero(): Unit = {
    leaderState.highestReplicatedIndex(config) mustBe 0
  }

  @Test
  def highestReplicatedIndexAfterAppendOn1ServersIsZero(): Unit = {
    val newLeaderState = leaderState.update(SERVER1, 2, 1)
    newLeaderState.highestReplicatedIndex(config) mustBe 0
  }

  @Test
  def highestReplicatedIndexAfterAppendO2ServersIsOne(): Unit = {
    val newlState = leaderState.update(SERVER1, 2, 1).update(SERVER2, 2, 1)
    newlState.highestReplicatedIndex(config) mustBe 1
  }

  @Test
  def highestReplicatedIndexAfterMultipleAppends(): Unit = {
    // matchIndex: (1 -> 2, 2 -> 1, 3 -> 0)
    val newlState = leaderState.update(SERVER1, 3, 2).update(SERVER2, 2, 1)
    newlState.highestReplicatedIndex(config) mustBe 1
  }
}
