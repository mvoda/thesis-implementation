package rx.distributed.raft.util

import com.google.protobuf.ByteString
import rx.distributed.raft.statemachine.output.StatemachineResponse

case class IndexedResponse(index: Int, response: StatemachineResponse)

object IndexedResponse {
  def apply(index: Int, serializedResponse: ByteString): IndexedResponse = {
    IndexedResponse(index, ByteStringDeserializer.deserialize[StatemachineResponse](serializedResponse))
  }
}
