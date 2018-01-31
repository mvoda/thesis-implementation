package rx.distributed.raft.converters

import com.google.protobuf.ByteString
import rx.distributed.raft.objectinputstreams.ClientObjectInputStream
import rx.distributed.raft.statemachine.input.StatemachineCommand

import scala.reflect.internal.util.ScalaClassLoader

object CommandConverters {
  def fromByteString(command: ByteString, classLoader: Option[ScalaClassLoader]): StatemachineCommand = {
    ClientObjectInputStream(command, classLoader).readObject().asInstanceOf[StatemachineCommand]
  }
}
