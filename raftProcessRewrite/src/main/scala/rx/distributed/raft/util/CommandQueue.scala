package rx.distributed.raft.util

import org.apache.logging.log4j.scala.Logging
import rx.lang.scala.Observable
import rx.lang.scala.subjects.ReplaySubject
import rx.lang.scala.subscriptions.CompositeSubscription

import scala.collection.JavaConverters._

case class CommandQueue(source: Observable[IndexedCommand]) extends Logging {
  private val subject = ReplaySubject[IndexedCommand]()
  private val receivedResponses: scala.collection.mutable.Set[Int] =
    java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap[Int, java.lang.Boolean]).asScala
  private val subscription = CompositeSubscription()

  val commands: Observable[IndexedCommand] =
    subject.filter(request => !receivedResponses.contains(request.index)).doOnSubscribe {
      if (!subscription.isUnsubscribed) {
        subscription += source.subscribe(subject)
      }
    }

  def unsubscribe(): Unit = subscription.unsubscribe()

  def addResponseSeqNum(seqNum: Int): Unit = receivedResponses.add(seqNum)
}
