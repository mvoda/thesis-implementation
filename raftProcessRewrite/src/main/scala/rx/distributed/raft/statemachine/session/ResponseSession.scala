package rx.distributed.raft.statemachine.session

import rx.distributed.raft.objectinputstreams.ClientClassLoader
import rx.distributed.raft.statemachine.output.StatemachineResponse

import scala.collection.immutable.SortedMap

case class ResponseSession(classLoader: Option[ClientClassLoader], map: SortedMap[Int, StatemachineResponse], discardedSeqNum: Int, timestamp: Long) {
  def getResponse(seqNum: Int): StatemachineResponse = map(seqNum)

  def saved(seqNum: Int): Boolean = map.contains(seqNum)

  def isDiscarded(seqNum: Int): Boolean = seqNum < discardedSeqNum

  def update(seqNum: Int, responseSeqNum: Int, response: StatemachineResponse, timestamp: Long): ResponseSession = {
    if (responseSeqNum > discardedSeqNum) {
      ResponseSession(classLoader, map.filterKeys(_ >= responseSeqNum) + (seqNum -> response), responseSeqNum, timestamp)
    }
    else ResponseSession(classLoader, map + (seqNum -> response), discardedSeqNum, timestamp)
  }

  def keepAlive(responseSeqNum: Int, newTimestamp: Long): ResponseSession = {
    if (responseSeqNum > discardedSeqNum)
      ResponseSession(classLoader, map.filterKeys(_ >= responseSeqNum), responseSeqNum, newTimestamp)
    else ResponseSession(classLoader, map, discardedSeqNum, newTimestamp)
  }
}
