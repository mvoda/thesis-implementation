package rx.distributed.observables

import com.google.protobuf.ByteString

trait JarResolver {
  def resolve(): Seq[ByteString]
}