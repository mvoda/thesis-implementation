package rx.distributed

import java.io.{File, PrintStream}
import java.util.concurrent.CountDownLatch

import rx.distributed.observables.ObservableExtensions._
import rx.distributed.observables.RemoteObservableExtensions._
import rx.distributed.raft.consensus.server.Address.{RemoteAddress, ServerAddress}
import rx.distributed.rpc.ClientServer
import rx.lang.scala.Observable

import scala.concurrent.duration.{Duration, SECONDS}

object ClientApp {

  case class RemoteFile(index: Int) {
    def fileName: File = new File(s"remote_file_$index.txt")
  }

  def main(args: Array[String]) {
    val selfSinkAddress = RemoteAddress("192.168.2.7", 5000)
    val selfCluster: Set[ServerAddress] = Set(RemoteAddress("192.168.2.7", 5051))
    val latch = new CountDownLatch(1)
    var counter = 0
    val remoteCluster: Set[ServerAddress] = Set(RemoteAddress("192.168.2.22", 5050))
    Observable.interval(Duration(1, SECONDS)).map(_ => {
      counter = counter + 1
      counter
    })
      .doOnUnsubscribe(latch.countDown())
      .observeOnRemote(remoteCluster)
      .map(_ * 10)
      .flatMap(v => {
        if (v <= 20) {
          Observable.error(new Exception())
        } else {
          Observable.just(v)
        }
      })
      .take(3)
      .retry()
      .observeOnRemote(selfCluster)
      .map(i => RemoteFile(i))
      .observeOnLocal(selfSinkAddress)
      .subscribe(
        (remoteFile: RemoteFile) => {
          val ps = new PrintStream(remoteFile.fileName)
          ps.println(s"Generated from remote jar. Index: ${remoteFile.index}")
          ps.close()
        })
    latch.await()
    ClientServer.get(selfSinkAddress).server.awaitTermination()
  }
}
