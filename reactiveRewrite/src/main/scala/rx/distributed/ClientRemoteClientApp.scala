package rx.distributed

import java.util.concurrent.CountDownLatch

import rx.distributed.observables.ObservableExtensions._
import rx.distributed.observables.RemoteObservableExtensions._
import rx.distributed.raft.consensus.server.Address.{RemoteAddress, ServerAddress}
import rx.lang.scala.Observable

import scala.concurrent.duration.{Duration, SECONDS}

object ClientRemoteClientApp {
  def main(args: Array[String]) {
    val addresses = readArgs(args)
    val selfAddress = addresses.head
    val latch = new CountDownLatch(1)
    var counter = 0
    val remoteCluster: Set[ServerAddress] = Set(addresses.tail: _*)
    Observable.interval(Duration(1, SECONDS)).map(_ => {
      counter = counter + 1
      println(s"produced: $counter")
      counter
    })
      .doOnUnsubscribe(latch.countDown())
      .observeOnRemote(remoteCluster)
      .filter(_ % 5 == 0)
      .map(_ * 10)
      .doOnNext(v => println(s"Remote produced: $v"))
      .observeOnLocal(selfAddress)
      .subscribe(
        value => println(s"Received value: $value"))
    latch.await()
  }

  def readArgs(args: Array[String]): Seq[ServerAddress] = {
    for (i <- args.indices by 2) yield RemoteAddress(args(i), args(i + 1).toInt)
  }
}
