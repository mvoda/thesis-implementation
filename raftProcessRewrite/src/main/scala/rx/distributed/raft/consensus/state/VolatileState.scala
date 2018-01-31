package rx.distributed.raft.consensus.state

/** Volatile State on all servers
  *
  * @param commitIndex index of highest log entry known to be committed (init to 0, increase monotonically)
  */
case class VolatileState(commitIndex: Int = 0, lastLeaderContact: Long = 0) {
  def updateCommitIndex(value: Int): VolatileState = VolatileState(value, lastLeaderContact)

  def updateLastLeaderContact(value: Long) = VolatileState(commitIndex, value)
}
