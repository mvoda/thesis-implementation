package rx.distributed.raft.statemachine.session

import rx.distributed.raft.objectinputstreams.ClientJar

case class ClientProducer(clientId: String, jars: Seq[ClientJar])
