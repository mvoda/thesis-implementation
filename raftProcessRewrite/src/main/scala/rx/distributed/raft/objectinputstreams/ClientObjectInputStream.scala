package rx.distributed.raft.objectinputstreams

import java.io.{ByteArrayInputStream, ObjectInputStream, ObjectStreamClass}

import com.google.protobuf.ByteString

import scala.reflect.internal.util.ScalaClassLoader

case class ClientObjectInputStream(bytes: ByteString, clientClassLoader: Option[ScalaClassLoader])
  extends ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray)) {

  override def resolveClass(objectStreamClass: ObjectStreamClass): Class[_] = {
    val classLoader = clientClassLoader.getOrElse(this.getClass.getClassLoader)
    val className = objectStreamClass.getName
    val clazz = Class.forName(className, false, classLoader)
    if (clazz != null)
      clazz
    else
      super.resolveClass(objectStreamClass)
  }
}