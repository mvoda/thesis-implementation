package rx.distributed.raft.events.rpc.request

import rx.distributed.raft.events.Event

trait RpcRequest[E <: Event] extends Event {
  def event: E
}
