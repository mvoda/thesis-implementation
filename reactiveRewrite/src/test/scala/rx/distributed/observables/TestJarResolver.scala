package rx.distributed.observables

import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging

object TestJarResolver extends JarResolver with Logging {
  def resolve(): Seq[ByteString] = Seq()
}
