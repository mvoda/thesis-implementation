package rx.distributed.raft.consensus

import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging
import org.junit.{After, Before, Test}
import org.scalatest.MustMatchers._
import rx.distributed.raft.Utils
import rx.distributed.raft.consensus.server.Address.TestingAddress
import rx.distributed.raft.consensus.state.PersistentState
import rx.distributed.raft.events.client.{ClientCommandEvent, KeepAliveEvent, RegisterClientEvent}
import rx.distributed.raft.log._
import rx.lang.scala.Observer
import rx.lang.scala.schedulers.ImmediateScheduler

import scala.util.Try

class PersistentStateIOTest extends Logging {
  private val ADDRESS = TestingAddress("test_server_1")
  private val CLUSTER_CONFIG = Utils.testCluster()

  @Before
  def beforeEach(): Unit = {
    Try(PersistentStateIO(ADDRESS, ImmediateScheduler()).cleanup())
  }

  @Test
  def readsWhatWasWritten(): Unit = {
    val logEntries = Seq(NoOpEntry(1, 1000),
      RegisterClientEntry(1, "client_1", RegisterClientEvent(Seq(), 2000)(Observer())),
      KeepAliveEntry(2, KeepAliveEvent("client_2", 1, 3000)(Observer())),
      ClientLogEntry(3, ClientCommandEvent("client_3", 1, 1, 4000, ByteString.copyFrom(Array[Byte](0)))(Observer()))
    )
    val log = RaftLog.create(logEntries, CLUSTER_CONFIG)
    val persistentState = PersistentState(1, Some(ADDRESS), log, None)
    val persistentStateIO = PersistentStateIO(ADDRESS, ImmediateScheduler())
    persistentStateIO.write(persistentState).subscribe(identity, e => logger.error(e))
    val readState = persistentStateIO.read(CLUSTER_CONFIG)
    readState.currentTerm mustBe persistentState.currentTerm
    readState.votedFor mustBe persistentState.votedFor
    readState.leaderHint mustBe persistentState.leaderHint
    readState.log mustBe persistentState.log
  }

  @After
  def afterEach(): Unit = {
    Try(PersistentStateIO(ADDRESS, ImmediateScheduler()).cleanup())
  }
}
