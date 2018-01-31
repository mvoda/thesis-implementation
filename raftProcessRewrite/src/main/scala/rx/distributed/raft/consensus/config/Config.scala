package rx.distributed.raft.consensus.config

/** Raft configuration
  *
  * @param timeoutMin election timeout min
  * @param timeoutMax election timeout max
  */
case class Config(timeoutMin: Int = 1501, timeoutMax: Int = 3000) {
  def heartbeatTimeout: Int = timeoutMin / 3

  // number of replication rounds
  def replicationRounds: Int = 10

  // expansion factor for snapshotting
  def expansionFactor: Int = 4

  // session timeout, in milliseconds
  def sessionTimeout: Int = 30000
}
