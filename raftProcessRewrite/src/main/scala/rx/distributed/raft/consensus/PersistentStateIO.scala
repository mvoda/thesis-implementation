package rx.distributed.raft.consensus

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.{Files, Paths}
import java.util.concurrent.Executors

import org.apache.logging.log4j.scala.Logging
import rx.distributed.raft.PersistentStateProto
import rx.distributed.raft.consensus.config.ClusterConfig
import rx.distributed.raft.consensus.server.Address.ServerAddress
import rx.distributed.raft.consensus.state.PersistentState
import rx.lang.scala.schedulers.ExecutionContextScheduler
import rx.lang.scala.{Observable, Scheduler}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

case class PersistentStateIO(address: ServerAddress,
                             scheduler: Scheduler = ExecutionContextScheduler(ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1))))
  extends Logging {
  def write(persistentState: PersistentState): Observable[Unit] = {
    val tempFile = File.createTempFile(fileName, ".tmp")
    Observable.just(persistentState).subscribeOn(scheduler).flatMap(state => Observable.from(Try[Unit]({
      state.toProtobuf().writeDelimitedTo(new FileOutputStream(tempFile))
    }).flatMap(_ => Try({
      Files.move(Paths.get(tempFile.getAbsolutePath), Paths.get(fileName), REPLACE_EXISTING)
      ()
    })))).doOnEach(_ => logger.info(s"[$address] Successfully written PersistentState."),
      error => logger.error(s"[$address] Failed to write PersistentState. Error message: $error"))
  }

  def read(clusterConfig: ClusterConfig): PersistentState = {
    Try({
      PersistentState.fromProtobuf(PersistentStateProto.parseDelimitedFrom(new FileInputStream(fileName)), clusterConfig)
    }) match {
      case Success(state) =>
        logger.info(s"[$address] Successfully read PersistentState.")
        state
      case Failure(error) =>
        logger.warn(s"[$address] Failed to read PersistentState. Error message: $error")
        throw error
    }
  }

  private def fileName: String = address.toString.replace(':', '-') + ".state"

  def cleanup(): Unit = Files.delete(Paths.get(fileName))
}
