package rx.distributed.raft.events.internal

import rx.distributed.raft.events.Event
import rx.distributed.raft.events.rpc.response.AppendEntriesResponse
import rx.lang.scala.Observer

trait RpcClientEvent extends Event

case class CountingAppendEntriesToServer(event: AppendEntriesToServer, activeLeaderObserver: Option[Observer[AppendEntriesResponse]]) extends RpcClientEvent
