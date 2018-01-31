package rx.distributed.raft.events.client

import com.google.protobuf.ByteString
import rx.distributed.raft.consensus.state.PersistentState
import rx.distributed.raft.events.rpc.request.ClientRpcRequest
import rx.distributed.raft.events.rpc.response.ClientCommandResponse
import rx.distributed.raft.log.{ClientLogEntry, LogEntry}
import rx.lang.scala.Observer

case class ClientCommandEvent(clientId: String, seqNum: Int, responseSeqNum: Int, timestamp: Long, command: ByteString)
                             (val observer: Observer[ClientCommandResponse]) extends ClientEvent with ClientRpcRequest {
  def toLogEntry(persistentState: PersistentState): LogEntry = ClientLogEntry(persistentState.currentTerm, this)

  override def event: ClientEvent = this
}
