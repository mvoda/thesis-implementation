package rx.distributed.raft.rpc.server

import rx.lang.scala.Observable

trait RPCServer[T] {
  def observable: Observable[T]
}
