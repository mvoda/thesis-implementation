package rx.distributed.raft.objectinputstreams

import java.io.{File, FileInputStream, FileOutputStream}
import java.net.URLClassLoader
import java.nio.file.Files

import com.google.common.io.Resources
import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging
import org.junit.Test
import org.scalatest.MustMatchers._
import rx.distributed.raft.converters.CommandConverters

import scala.util.{Success, Try}

class ClientObjectInputStreamTest extends Logging {
  private val CLIENT_JAR_PATH = Resources.getResource("clientApplication.jar").getPath
  private val COMMAND_FILE_PATH = Resources.getResource("command_1.bin").getPath

  @Test
  def deserializesClientCommand(): Unit = {
    val clientId = "client"
    val jarByteString = ByteString.readFrom(new FileInputStream(CLIENT_JAR_PATH))
    jarByteString.writeTo(new FileOutputStream(clientId))
    val classLoader = new URLClassLoader(Array(new File(clientId).toURI.toURL), this.getClass.getClassLoader)
    val command = ByteString.readFrom(new FileInputStream(COMMAND_FILE_PATH))
    val deserializedCommand = Try(CommandConverters.fromByteString(command, Some(classLoader)))
    jarByteString.size() > 0 mustBe true
    deserializedCommand mustBe an[Success[_]]
    Files.delete(new File("client").toPath)
  }
}
