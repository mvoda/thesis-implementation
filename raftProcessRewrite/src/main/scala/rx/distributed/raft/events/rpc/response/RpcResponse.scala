package rx.distributed.raft.events.rpc.response

import com.google.protobuf.MessageOrBuilder

trait RpcResponse {
  def toProtobuf: MessageOrBuilder
}



