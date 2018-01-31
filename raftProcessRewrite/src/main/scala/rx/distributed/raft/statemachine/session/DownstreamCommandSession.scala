package rx.distributed.raft.statemachine.session

import rx.distributed.raft.statemachine.input.StatemachineCommand

case class DownstreamCommandSession(producerExpired: Boolean, clientId: Option[String], startingSeqNum: Int, commands: Seq[StatemachineCommand]) {
  // remove commands up to seqNum (non-inclusive)
  def removeCommands(seqNum: Int): DownstreamCommandSession = {
    if (seqNum > startingSeqNum)
      DownstreamCommandSession(producerExpired, clientId, seqNum, commands.drop(seqNum - startingSeqNum))
    else this
  }

  def addCommands(newCommands: Seq[StatemachineCommand]): DownstreamCommandSession =
    DownstreamCommandSession(producerExpired, clientId, startingSeqNum, commands ++ newCommands)

  def addCommand(command: StatemachineCommand): DownstreamCommandSession = addCommands(Seq(command))

  def expire(): DownstreamCommandSession = DownstreamCommandSession(producerExpired = true, clientId, startingSeqNum, commands)

  def registered(clientId: String) = DownstreamCommandSession(producerExpired, Some(clientId), startingSeqNum, commands)

  def isCompleted: Boolean = producerExpired && commands.isEmpty
}
