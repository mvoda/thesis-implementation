package rx.distributed.raft.events.internal

import rx.distributed.raft.consensus.server.Address.ServerAddress
import rx.distributed.raft.consensus.state.PersistentState
import rx.distributed.raft.events.Event
import rx.distributed.raft.events.consensus.{AppendEntries, VoteRequest}
import rx.distributed.raft.events.rpc.response.{AppendEntriesSuccess, VoteGranted}

sealed trait InternalEvent extends Event

case class ResetElectionTimeout() extends InternalEvent

case class ResetHeartbeatTimeout() extends InternalEvent

case class StopHeartbeatTimeout() extends InternalEvent

case class CommitEntries(commitIndex: Int, newCommitIndex: Int) extends InternalEvent

case class SelfVoteGranted(voteGranted: VoteGranted) extends InternalEvent

case class SelfAppendEntriesSuccess(appendEntriesSuccess: AppendEntriesSuccess) extends InternalEvent

case class SyncToDiskAndThen(persistentState: PersistentState, nextEvent: InternalEvent) extends InternalEvent

sealed trait Response extends InternalEvent

final case class Reject() extends Response

final case class Accept() extends Response

case class NotLeader() extends InternalEvent

case class AppendEntriesToServer(server: ServerAddress, appendEntries: AppendEntries) extends InternalEvent

case class RequestVotesFromServer(server: ServerAddress, voteRequest: VoteRequest) extends InternalEvent with RpcClientEvent
