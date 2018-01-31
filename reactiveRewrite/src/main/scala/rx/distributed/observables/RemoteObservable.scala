package rx.distributed.observables

import rx.distributed.observers.remote.StreamSetupObserver
import rx.distributed.raft.consensus.server.Address.ServerAddress

trait RemoteObservable[T] {
  def cluster: Set[ServerAddress]

  def subscribe(observer: StreamSetupObserver[T]): Unit
}
