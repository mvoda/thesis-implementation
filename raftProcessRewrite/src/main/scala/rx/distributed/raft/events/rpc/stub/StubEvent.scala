package rx.distributed.raft.events.rpc.stub

import rx.distributed.raft.RaftClientGrpc.RaftClientStub
import rx.distributed.raft.consensus.server.Address.ServerAddress

sealed trait StubEvent

case class SwitchChannel(stub: RaftClientStub, leaderHint: Option[ServerAddress] = None) extends StubEvent

case class CreateNewStub() extends StubEvent