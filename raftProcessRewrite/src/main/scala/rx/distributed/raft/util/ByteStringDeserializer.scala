package rx.distributed.raft.util

import java.io.{ByteArrayInputStream, ObjectInputStream}

import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging

import scala.util.{Failure, Success, Try}

object ByteStringDeserializer extends Logging {
  def deserialize[T <: Serializable](serializedMessage: ByteString): T = {
    Try({
      if (serializedMessage == ByteString.EMPTY) {
        return null.asInstanceOf[T]
      } else {
        new ObjectInputStream(new ByteArrayInputStream(serializedMessage.toByteArray)).readObject().asInstanceOf[T]
      }
    }) match {
      case Success(message) => message
      case Failure(e) =>
        logger.error(s"Deserialization failed with message: ${e.getMessage}")
        throw e
    }
  }
}
