package rx.distributed.raft.events.consensus

import rx.distributed.raft.consensus.server.Address.ServerAddress
import rx.distributed.raft.events.Event

trait ConsensusEvent extends Event {
  def term: Int

  def address: ServerAddress
}
