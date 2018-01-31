package rx.distributed.raft.forwarding

import rx.distributed.raft.statemachine.input.StatemachineCommand
import rx.distributed.raft.util.ResponseTracker
import rx.lang.scala.{Subject, Subscription}

case class ForwardingClientSource(source: Subject[StatemachineCommand], responseTracker: ResponseTracker, subscription: Subscription)
