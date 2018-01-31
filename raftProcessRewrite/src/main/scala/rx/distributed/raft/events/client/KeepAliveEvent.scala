package rx.distributed.raft.events.client

import rx.distributed.raft.consensus.state.PersistentState
import rx.distributed.raft.events.rpc.request.ClientRpcRequest
import rx.distributed.raft.events.rpc.response.KeepAliveResponse
import rx.distributed.raft.log.{KeepAliveEntry, LogEntry}
import rx.lang.scala.Observer

case class KeepAliveEvent(clientId: String, responseSeqNum: Int, timestamp: Long)
                         (val observer: Observer[KeepAliveResponse]) extends ClientEvent with ClientRpcRequest {
  def toLogEntry(persistentState: PersistentState): LogEntry = KeepAliveEntry(persistentState.currentTerm, this)

  override def event: ClientEvent = this
}
