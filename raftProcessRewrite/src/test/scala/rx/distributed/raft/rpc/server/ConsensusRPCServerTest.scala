package rx.distributed.raft.rpc.server

import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.logging.{Level, Logger}

import io.grpc.inprocess.InProcessChannelBuilder
import org.junit.Test
import org.scalatest.MustMatchers._
import rx.distributed.raft.LatchObservers.OnNextCountdown
import rx.distributed.raft._
import rx.distributed.raft.events.consensus.{AppendEntries, VoteRequest}
import rx.distributed.raft.events.rpc.request.ConsensusRpcRequest

class ConsensusRPCServerTest {
  Logger.getLogger("io.grpc").setLevel(Level.OFF)

  def createFutureStub(name: String): RaftGrpc.RaftFutureStub = {
    RaftGrpc.newFutureStub(InProcessChannelBuilder.forName(name).build())
  }

  @Test
  def sendsAppendEntries(): Unit = {
    val eventObserver = OnNextCountdown[ConsensusRpcRequest](1)
    val serverName = UUID.randomUUID().toString
    val stub = createFutureStub(serverName)
    val subscription = Utils.createConsensusServer(serverName).subscribe(eventObserver.observer)
    stub.appendEntries(AppendEntriesRequestProto.newBuilder().build())
    eventObserver.latch.await(3, TimeUnit.SECONDS)
    eventObserver.events.size mustBe 1
    eventObserver.events.head.event mustBe an[AppendEntries]
    subscription.unsubscribe()
  }

  @Test
  def sendsRequestVote(): Unit = {
    val eventObserver = OnNextCountdown[ConsensusRpcRequest](1)
    val serverName = UUID.randomUUID().toString
    val stub = createFutureStub(serverName)
    val subscription = Utils.createConsensusServer(serverName).subscribe(eventObserver.observer)
    stub.requestVote(VoteRequestProto.newBuilder().build())
    eventObserver.latch.await(3, TimeUnit.SECONDS)
    eventObserver.events.size mustBe 1
    eventObserver.events.head.event mustBe an[VoteRequest]
    subscription.unsubscribe()
  }
}
