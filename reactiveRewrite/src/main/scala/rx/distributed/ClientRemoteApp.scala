package rx.distributed

import java.util.concurrent.CountDownLatch

import rx.distributed.observables.ObservableExtensions._
import rx.distributed.observables.RemoteObservableExtensions._
import rx.distributed.raft.consensus.server.Address.{RemoteAddress, ServerAddress}
import rx.lang.scala.Observable

import scala.concurrent.duration.{Duration, SECONDS}

object ClientRemoteApp {
  def main(args: Array[String]) {
    val latch = new CountDownLatch(1)
    var counter = 0
    val remoteCluster: Set[ServerAddress] = Set(RemoteAddress("192.168.2.22", 5050), RemoteAddress("192.168.2.22", 5051), RemoteAddress("192.168.2.22", 5052))
    Observable.interval(Duration(1, SECONDS)).map(_ => {
      counter = counter + 1
      println(s"produced event: $counter")
      counter
    })
      .doOnUnsubscribe(latch.countDown())
      .observeOnRemote(remoteCluster)
      .filter(_ % 5 == 0)
      .map(_ * 10)
      .take(5)
      .subscribe(
        (value: Int) => println(s"Received value: $value"),
        (error: Throwable) => Console.err.println(s"Received error: $error"),
        () => println("Received onCompleted"))
    latch.await()
  }
}
