package rx.distributed.raft.util

import com.google.protobuf.ByteString
import rx.distributed.raft.ClientRequestProto
import rx.distributed.raft.statemachine.input.StatemachineCommand

case class IndexedCommand(index: Int, command: StatemachineCommand) {
  def toProtobuf(clientId: String, responseSequenceNum: Int): ClientRequestProto = {
    val serializedCommand = if (command == null) ByteString.EMPTY else ByteStringSerializer.write(command)
    ClientRequestProto.newBuilder().setClientId(clientId).setSequenceNum(index).setResponseSequenceNum(responseSequenceNum)
      .setCommand(serializedCommand).build()
  }
}
