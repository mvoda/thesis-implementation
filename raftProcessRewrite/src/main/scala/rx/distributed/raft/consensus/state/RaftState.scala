package rx.distributed.raft.consensus.state

import rx.distributed.raft.events.rpc.response.VoteGranted

object RaftState {

  sealed trait RaftState

  case class Leader(leaderState: LeaderState) extends RaftState

  case class Candidate(votes: Set[VoteGranted] = Set()) extends RaftState

  case class Follower() extends RaftState

  case object NonVoting extends RaftState

}
