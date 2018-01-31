package rx.distributed.raft.converters

import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}
import rx.lang.scala.Observable

object ObservableConverters {

  def fromFuture[T](future: ListenableFuture[T]): Observable[T] = {
    Observable(subscriber => {
      val lock = new Object()
      Futures.addCallback(future, new FutureCallback[T] {
        override def onFailure(throwable: Throwable): Unit = {
          lock.synchronized {
            if (!subscriber.isUnsubscribed) subscriber.onError(throwable)
          }
        }

        override def onSuccess(t: T): Unit = {
          lock.synchronized {
            if (!subscriber.isUnsubscribed) {
              subscriber.onNext(t)
              subscriber.onCompleted()
            }
          }
        }
      })
    })
  }
}
