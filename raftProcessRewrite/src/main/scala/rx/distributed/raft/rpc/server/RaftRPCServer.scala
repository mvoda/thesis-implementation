package rx.distributed.raft.rpc.server

import io.grpc.Server
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.netty.NettyServerBuilder
import org.apache.logging.log4j.scala.Logging
import rx.distributed.raft.consensus.server.Address.{RemoteAddress, ServerAddress, TestingAddress}
import rx.distributed.raft.events.Event
import rx.lang.scala.Notification.OnNext
import rx.lang.scala.Observable
import rx.lang.scala.observables.AsyncOnSubscribe

case class RaftRPCServer(address: ServerAddress) extends RPCServer[Event] with Logging {
  private val commandServer = CommandRPCServer()
  private val consensusServer = ConsensusRPCServer()
  private var server: Server = _

  override val observable: Observable[Event] =
    Observable.create(AsyncOnSubscribe.singleState(() => start())
    ((_, _) => OnNext(commandServer.observable.merge(consensusServer.observable)), (server) => shutdown(server)))

  private def start(): Server = {
    logger.info(s"[$address] Starting RPC Server")
    address match {
      case TestingAddress(name) =>
        server = InProcessServerBuilder.forName(name).addService(commandServer).addService(consensusServer).build()
      case RemoteAddress(_, port) =>
        server = NettyServerBuilder.forPort(port).maxMessageSize(104857600).addService(commandServer).addService(consensusServer).build()
    }
    server.start()
    logger.info(s"[$address] RPC Server successfully started")
    server
  }

  private def shutdown(server: Server): Unit = {
    logger.info(s"[$address] Shutting down RPC Server")
    server.shutdownNow()
    server.awaitTermination()
    logger.info(s"[$address] RPC Server shut down")
  }

  def await(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }
}
