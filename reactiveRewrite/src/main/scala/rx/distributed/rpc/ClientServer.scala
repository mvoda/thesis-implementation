package rx.distributed.rpc

import rx.distributed.raft.consensus.server.Address.ServerAddress

object ClientServer {
  private var servers: Map[ServerAddress, RPCServer] = Map()

  def get(address: ServerAddress): RPCServer = {
    this.synchronized {
      servers.get(address) match {
        case None => createServer(address)
        case Some(server) if server.server.isShutdown || server.server.isTerminated => createServer(address)
        case Some(server) => server
      }
    }
  }

  private def createServer(address: ServerAddress): RPCServer = {
    val server = new RPCServer(address)
    servers = servers + (address -> server)
    server.start()
    server
  }
}
