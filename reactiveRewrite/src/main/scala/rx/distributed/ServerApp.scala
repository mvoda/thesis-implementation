package rx.distributed

import rx.distributed.raft.RaftFsm
import rx.distributed.raft.consensus.config.{ClusterConfig, Config}
import rx.distributed.raft.consensus.server.Address.RemoteAddress
import rx.distributed.raft.consensus.server.ServerInfo.VotingServer
import rx.distributed.statemachine.RxStatemachine

object ServerApp {
  def main(args: Array[String]) {
    val address = RemoteAddress(args(0), args(1).toInt)
    val config = ClusterConfig(Set(VotingServer(address)))
    val raftFsm = RaftFsm(address, Config(), config, RxStatemachine(60000))
    raftFsm.rpcServer.await()
  }
}
