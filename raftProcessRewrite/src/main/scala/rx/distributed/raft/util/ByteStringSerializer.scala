package rx.distributed.raft.util

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging

import scala.util.{Failure, Success, Try}

object ByteStringSerializer extends Logging {
  def write[T <: Serializable](command: T): ByteString = {
    Try({
      val byteStream = new ByteArrayOutputStream()
      new ObjectOutputStream(byteStream).writeObject(command)
      byteStream.toByteArray
    }) match {
      case Success(bytes) => ByteString.copyFrom(bytes)
      case Failure(e) =>
        logger.error(s"Serialization of command $command failed with message: ${e.getMessage}")
        throw e
    }
  }
}
