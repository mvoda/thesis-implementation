package rx.distributed.observables

import java.util.UUID

import org.apache.logging.log4j.scala.Logging
import rx.distributed.observers.remote.StreamSetupObserver
import rx.distributed.observers.statemachine.{DownstreamStatemachineObserver, LocalStatemachineObserver, StatemachineObserver}
import rx.distributed.raft.consensus.server.Address.ServerAddress
import rx.distributed.raft.converters.ObserverConverters
import rx.distributed.raft.rpc.client.RaftClient
import rx.distributed.raft.statemachine.output.StatemachineResponse
import rx.distributed.statemachine.commands.{RxEvent, SetUpstreamForStreamId, Subscribe}
import rx.distributed.statemachine.responses.Unsubscribed
import rx.lang.scala.{Observable, Subscriber}

object ObservableExtensions extends Logging {
  var jarResolver: JarResolver = SelfJarResolver

  implicit class ObservableOnRaftExtensions[T](val src: Observable[T]) {
    def observeOnRemote(raftCluster: Set[ServerAddress]): RemoteObservable[T] = {
      new RemoteObservable[T] {
        override def cluster: Set[ServerAddress] = raftCluster

        override def subscribe(observer: StreamSetupObserver[T]): Unit = {
          val streamId = UUID.randomUUID().toString
          val raftClient = RaftClient(raftCluster, 60, 1, jarResolver.resolve())
          val subscribeCommand = Subscribe(streamId, observer.operators, observer.statemachineObserver)
          val responseSubscriber = createSubscriber()
          (raftClient.sendCommands(Observable.just(subscribeCommand))
            ++ setUpstream(raftCluster, streamId, observer.statemachineObserver)
            ++ raftClient.sendCommands(src.materialize.map(n => RxEvent(streamId, n))))
            .subscribe(responseSubscriber)
        }
      }
    }

    private def createSubscriber(): Subscriber[StatemachineResponse] =
      ObserverConverters.makeSafe(new Subscriber[StatemachineResponse]() {
        override def onNext(value: StatemachineResponse): Unit = value match {
          case Unsubscribed(_) => unsubscribe()
          case _ =>
        }
      })
  }

  def setUpstream(upstreamCluster: Set[ServerAddress], upstreamStreamId: String,
                  statemachineObserver: StatemachineObserver): Observable[StatemachineResponse] = statemachineObserver match {
    case LocalStatemachineObserver(_, _, _) => Observable.empty
    case DownstreamStatemachineObserver(downstreamStreamId, cluster) =>
      val downstreamClient = RaftClient(cluster, 60, 1, Seq())
      val command = SetUpstreamForStreamId(downstreamStreamId, upstreamCluster, upstreamStreamId)
      downstreamClient.sendCommands(Observable.just(command))
  }

}
