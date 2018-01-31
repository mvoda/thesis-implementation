package rx.distributed.raft.events.consensus

import rx.distributed.raft.events.Event

case class HeartbeatTimeout() extends Event
