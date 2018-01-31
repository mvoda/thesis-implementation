package rx.distributed.observables

import java.io.{File, FileInputStream}

import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging

object SelfJarResolver extends JarResolver with Logging {
  def resolve(): Seq[ByteString] = {
    val jarFile = new File(System.getProperty("distributedRx.jar"))
    if (!jarFile.exists() || !jarFile.isFile) {
      logger.error("Could not locate jar file. You must package and run this application from a jar with dependencies.")
      System.exit(1)
    }
    Seq(ByteString.readFrom(new FileInputStream(jarFile)))
  }
}
