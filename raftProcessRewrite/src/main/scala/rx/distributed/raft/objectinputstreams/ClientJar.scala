package rx.distributed.raft.objectinputstreams

import java.io.{File, FileInputStream}
import java.net.URL
import java.nio.file.Paths

import com.google.protobuf.ByteString
import rx.distributed.raft.consensus.server.Address.ServerAddress

case class ClientJar(fileName: String)(address: ServerAddress) {
  def path: String = System.getProperty("java.io.tmpdir") + File.separator + s"${address.toString}_$fileName"

  def url: URL = Paths.get(path).toUri.toURL

  def bytes: ByteString = ByteString.readFrom(new FileInputStream(path))
}
