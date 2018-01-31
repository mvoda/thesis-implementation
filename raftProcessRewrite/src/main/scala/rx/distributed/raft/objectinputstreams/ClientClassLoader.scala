package rx.distributed.raft.objectinputstreams

import java.net.URLClassLoader

import scala.reflect.internal.util.ScalaClassLoader

case class ClientClassLoader(jars: Seq[ClientJar], classLoader: ScalaClassLoader)

object ClientClassLoader {
  def apply(jars: Seq[ClientJar]): ClientClassLoader = {
    ClientClassLoader(jars, new URLClassLoader(jars.map(_.url).toArray, this.getClass.getClassLoader))
  }
}
