package rx.distributed.raft.events.forwarding

import rx.distributed.raft.events.Event

trait ForwardingModuleEvent extends Event

case class LeaderSteppedDown() extends ForwardingModuleEvent
