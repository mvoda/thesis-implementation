package rx.distributed.raft.events.client

import com.google.protobuf.ByteString
import rx.distributed.raft.consensus.state.PersistentState
import rx.distributed.raft.events.rpc.request.ClientRpcRequest
import rx.distributed.raft.events.rpc.response.RegisterClientResponse
import rx.distributed.raft.log.{LogEntry, RegisterClientEntry}
import rx.lang.scala.Observer

case class RegisterClientEvent(jarsBytes: Seq[ByteString], timestamp: Long)(val observer: Observer[RegisterClientResponse]) extends ClientEvent with ClientRpcRequest {
  def toLogEntry(persistentState: PersistentState): LogEntry =
    RegisterClientEntry(persistentState.currentTerm, (persistentState.log.lastIndex + 1).toString, this)

  override def event: ClientEvent = this
}
