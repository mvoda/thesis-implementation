package rx.distributed.raft.util

import java.util.concurrent.atomic.AtomicInteger

import org.apache.logging.log4j.scala.Logging

case class ResponseTracker(commandQueue: CommandQueue, startingResponseSeqNum: Int) extends Logging {
  private val nextExpectedResponse = new AtomicInteger(startingResponseSeqNum)

  def addResponse(seqNum: Int): Unit = {
    if (nextExpectedResponse.compareAndSet(seqNum, seqNum + 1)) {
      commandQueue.addResponseSeqNum(seqNum)
    }
  }

  def get(): Int = nextExpectedResponse.get()
}
