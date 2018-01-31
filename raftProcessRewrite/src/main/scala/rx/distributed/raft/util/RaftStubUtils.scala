package rx.distributed.raft.util

import java.util.concurrent.TimeUnit

import io.grpc.ManagedChannel
import io.grpc.stub.AbstractStub

object RaftStubUtils {
  def terminateStubChannel(stub: AbstractStub[_]): Unit = {
    val channel = stub.getChannel.asInstanceOf[ManagedChannel]
    channel.shutdownNow()
    channel.awaitTermination(Long.MaxValue, TimeUnit.SECONDS)
  }
}
