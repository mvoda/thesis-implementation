package rx.distributed.observables

import java.util.UUID

import org.apache.logging.log4j.scala.Logging
import rx.distributed.observers.remote.StreamSetupObserver
import rx.distributed.observers.statemachine.{DownstreamStatemachineObserver, LocalStatemachineObserver}
import rx.distributed.operators.combining._
import rx.distributed.operators.single._
import rx.distributed.raft.consensus.server.Address.ServerAddress
import rx.distributed.raft.rpc.client.RaftClient
import rx.distributed.rpc.ClientServer
import rx.distributed.statemachine.commands.Subscribe
import rx.lang.scala.{Observable, Subscription}

object RemoteObservableExtensions extends Logging {
  var jarResolver: JarResolver = SelfJarResolver

  implicit class RemoteObservableOperatorExtensions[T](val src: RemoteObservable[T]) {
    def map[S](fun: (T) => S): RemoteObservable[S] = lift(_.addOperator(Map(fun)))

    def filter(predicate: (T) => Boolean): RemoteObservable[T] = lift(_.addOperator(Filter(predicate)))

    def take(n: Int): RemoteObservable[T] = lift(_.addOperator(Take[T](n)))

    def flatMap[S](fun: (T) => Observable[S]): RemoteObservable[S] = lift(_.addOperator(FlatMap(fun)))

    def retry(): RemoteObservable[T] = lift(_.addOperator(Retry[T]()))

    def distinctUntilChanged: RemoteObservable[T] = lift(_.addOperator(DistinctUntilChanged[T]()))

    def slidingBuffer(count: Int, skip: Int): RemoteObservable[Seq[T]] = lift(_.addOperator(SlidingBuffer[T](count, skip)))

    def scan[S](initialValue: S)(accumulator: (S, T) => S): RemoteObservable[S] = lift(_.addOperator(Scan(initialValue)(accumulator)))

    def doOnNext(action: T => Unit): RemoteObservable[T] = lift(_.addOperator(DoOnNext(action)))

    def lift[S](transform: StreamSetupObserver[S] => StreamSetupObserver[T]): RemoteObservable[S] = {
      new RemoteObservable[S] {
        override def cluster: Set[ServerAddress] = src.cluster

        override def subscribe(observer: StreamSetupObserver[S]): Unit = src.subscribe(transform(observer))
      }
    }

    def observeOnSameRemote[R](other: RemoteObservable[R]): RemoteObservable[T] =
      if (other.cluster == src.cluster) src
      else src.observeOnRemote(other.cluster)


    def merge[S >: T](that: RemoteObservable[S]): RemoteObservable[S] = {
      new RemoteObservable[S] {
        override def cluster: Set[ServerAddress] = src.cluster

        override def subscribe(observer: StreamSetupObserver[S]): Unit = {
          val operatorId = UUID.randomUUID().toString
          that.observeOnSameRemote(src).subscribe(observer.addOperator[S, S](Merge(operatorId, RightStream())))
          src.subscribe(observer.addOperator[T, S](Merge(operatorId, LeftStream())))
        }
      }
    }


    def concat[S >: T](that: RemoteObservable[S]): RemoteObservable[S] = {
      new RemoteObservable[S] {
        override def cluster: Set[ServerAddress] = src.cluster

        override def subscribe(observer: StreamSetupObserver[S]): Unit = {
          val operatorId = UUID.randomUUID().toString
          that.observeOnSameRemote(src).subscribe(observer.addOperator[S, S](Concat(operatorId, RightStream())))
          src.subscribe(observer.addOperator[T, S](Concat(operatorId, LeftStream())))
        }
      }
    }

    def tumblingBuffer[S](that: => RemoteObservable[S]): RemoteObservable[Seq[T]] = {
      new RemoteObservable[Seq[T]] {
        override def cluster: Set[ServerAddress] = src.cluster

        override def subscribe(observer: StreamSetupObserver[Seq[T]]): Unit = {
          val operatorId = UUID.randomUUID().toString
          that.observeOnSameRemote(src).subscribe(observer.addOperatorRight[T, S](TumblingBuffer(operatorId, RightStream())))
          src.subscribe(observer.addOperatorLeft[T, S](TumblingBuffer(operatorId, LeftStream())))
        }
      }
    }

    def combineLatestWith[S, R](that: RemoteObservable[S])(combiner: (T, S) => R): RemoteObservable[R] = {
      new RemoteObservable[R] {
        override def cluster: Set[ServerAddress] = src.cluster

        override def subscribe(observer: StreamSetupObserver[R]): Unit = {
          val operatorId = UUID.randomUUID().toString
          that.observeOnSameRemote(src).subscribe(observer.addOperatorRight[T, S](CombineLatestWith(operatorId, RightStream(), combiner)))
          src.subscribe(observer.addOperatorLeft[T, S](CombineLatestWith(operatorId, LeftStream(), combiner)))
        }
      }
    }

    def observeOnLocal(address: ServerAddress): Observable[T] = {
      Observable(subscriber => {
        val server = ClientServer.get(address)
        val clientId = server.generateId(subscriber)
        val statemachineObserver = DownstreamStatemachineObserver(clientId, Set(address))
        src.subscribe(StreamSetupObserver(Seq(), statemachineObserver))
      })
    }

    def observeOnRemote(raftCluster: Set[ServerAddress]): RemoteObservable[T] = {
      new RemoteObservable[T] {
        override def cluster: Set[ServerAddress] = raftCluster

        override def subscribe(observer: StreamSetupObserver[T]): Unit = {
          val streamId = UUID.randomUUID().toString
          val raftClient = RaftClient(raftCluster, 60, 1, jarResolver.resolve())
          val subscribeCommand = Subscribe(streamId, observer.operators, observer.statemachineObserver)
          (raftClient.sendCommands(Observable.just(subscribeCommand))
            ++ ObservableExtensions.setUpstream(raftCluster, streamId, observer.statemachineObserver))
            .subscribe(_ => (),
              e => logger.error(s"RemoteObservable.observeOnRaft observable produced error: $e"),
              () => {
                val statemachineObserver = DownstreamStatemachineObserver(streamId, raftCluster)
                src.subscribe(StreamSetupObserver(Seq(), statemachineObserver))
              }
            )
        }
      }
    }

    def subscribe(onNext: T => Unit): Subscription = {
      subscribe(LocalStatemachineObserver((value: Any) => onNext(value.asInstanceOf[T])))
    }

    def subscribe(onNext: T => Unit, onError: Throwable => Unit): Subscription = {
      subscribe(LocalStatemachineObserver((value: Any) => onNext(value.asInstanceOf[T]), onError))
    }

    def subscribe(onNext: T => Unit, onError: Throwable => Unit, onCompleted: () => Unit): Subscription = {
      subscribe(LocalStatemachineObserver((value: Any) => onNext(value.asInstanceOf[T]), onError, onCompleted))
    }

    private def subscribe(statemachineObserver: LocalStatemachineObserver): Subscription = {
      src.subscribe(StreamSetupObserver(Seq(), statemachineObserver))
      Subscription {
        //TODO: How to unsubscribe this?
      }
    }
  }

}
