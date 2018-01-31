package rx.distributed.raft.statemachine.session

import org.junit.Test
import org.scalatest.MustMatchers._
import rx.distributed.raft.client.RaftClusterClient
import rx.distributed.raft.consensus.server.Address.TestingAddress
import rx.distributed.raft.statemachine.output.StatemachineOutputUtils.{EmptyResponse, ForwardingEmptyResponse}

class StatemachineSessionsTest {
  private val TIMEOUT = 1000
  private val TIMESTAMP = 1000
  private val CLIENT_ID = "client"
  private val DOWNSTREAM_ADDRESS = TestingAddress("server_1")
  private val DOWNSTREAM_REGISTERED_CLIENT = RaftClusterClient(Set(DOWNSTREAM_ADDRESS), "registered-id")
  private val DOWNSTREAM_PRODUCER = RaftClusterProducer(Set(DOWNSTREAM_ADDRESS), ClientProducer(CLIENT_ID, Seq()))

  @Test
  def addsNewResponseSession(): Unit = {
    val session = StatemachineSessions(TIMEOUT)
    session.addNewSession(CLIENT_ID, None, TIMESTAMP)
    session.registeredClients.size mustBe 1
    session.registeredClients.contains(CLIENT_ID) mustBe true
  }

  @Test
  def expiresResponseSessionAfterTimeout(): Unit = {
    val session = StatemachineSessions(TIMEOUT)
    session.addNewSession(CLIENT_ID, None, TIMESTAMP)
    session.removeExpired(TIMESTAMP + TIMEOUT)
    session.registeredClients.size mustBe 0
  }

  @Test
  def doesNotExpireResponseSessionBeforeTimeout(): Unit = {
    val session = StatemachineSessions(TIMEOUT)
    session.addNewSession(CLIENT_ID, None, TIMESTAMP)
    session.removeExpired(TIMESTAMP + TIMEOUT - 1)
    session.registeredClients.size mustBe 1
    session.registeredClients.contains(CLIENT_ID) mustBe true
  }

  @Test
  def keepsSessionAliveOnKeepAliveMessage(): Unit = {
    val session = StatemachineSessions(TIMEOUT)
    session.addNewSession(CLIENT_ID, None, TIMESTAMP)
    session.keepAlive(CLIENT_ID, 0, TIMESTAMP + TIMEOUT - 1)
    session.removeExpired(TIMESTAMP + TIMEOUT + TIMEOUT - 2)
    session.registeredClients.size mustBe 1
    session.registeredClients.contains(CLIENT_ID) mustBe true
  }

  @Test
  def keepsSessionAliveOnUpdate(): Unit = {
    val session = StatemachineSessions(TIMEOUT)
    session.addNewSession(CLIENT_ID, None, TIMESTAMP)
    session.updateSession(CLIENT_ID, 1, 0, EmptyResponse(), TIMESTAMP + TIMEOUT - 1)
    session.removeExpired(TIMESTAMP + TIMEOUT + TIMEOUT - 2)
    session.registeredClients.size mustBe 1
    session.registeredClients.contains(CLIENT_ID) mustBe true
  }

  @Test
  def addsDownstreamCommandToSession(): Unit = {
    val session = StatemachineSessions(TIMEOUT)
    session.addNewSession(CLIENT_ID, None, TIMESTAMP)
    session.updateSession(CLIENT_ID, 1, 0, ForwardingEmptyResponse(Set(DOWNSTREAM_ADDRESS)), TIMESTAMP + TIMEOUT - 1)
    session.removeExpired(TIMESTAMP + TIMEOUT + TIMEOUT - 2)
    session.registeredClients.size mustBe 1
    session.registeredClients.contains(CLIENT_ID) mustBe true
    session.commandSessions.producers.size mustBe 1
    session.commandSessions.producers.contains(DOWNSTREAM_PRODUCER) mustBe true
    session.commandSessions.commandSession(DOWNSTREAM_PRODUCER).commands.size mustBe 1
  }

  @Test
  def removesDeliveredCommand(): Unit = {
    val session = StatemachineSessions(TIMEOUT)
    session.addNewSession(CLIENT_ID, None, TIMESTAMP)
    session.updateSession(CLIENT_ID, 1, 0, ForwardingEmptyResponse(Set(DOWNSTREAM_ADDRESS)), TIMESTAMP + TIMEOUT - 1)
    session.registeredDownstreamClient(DOWNSTREAM_PRODUCER, DOWNSTREAM_REGISTERED_CLIENT.id)
    session.removeCommands(DOWNSTREAM_REGISTERED_CLIENT, 2)
    session.commandSessions.producers.size mustBe 1
    session.commandSessions.producers.contains(DOWNSTREAM_PRODUCER) mustBe true
    session.commandSessions.commandSession(DOWNSTREAM_PRODUCER).commands.size mustBe 0
  }

  // does not expire commandSession if producer is alive but command list is empty
  @Test
  def doesNotExpireCommandSessionAliveProducerEmptyCommands(): Unit = {
    val session = StatemachineSessions(TIMEOUT)
    session.addNewSession(CLIENT_ID, None, TIMESTAMP)
    session.updateSession(CLIENT_ID, 1, 0, ForwardingEmptyResponse(Set(DOWNSTREAM_ADDRESS)), TIMESTAMP + TIMEOUT - 1)
    session.registeredDownstreamClient(DOWNSTREAM_PRODUCER, DOWNSTREAM_REGISTERED_CLIENT.id)
    session.removeCommands(DOWNSTREAM_REGISTERED_CLIENT, 2)
    session.removeExpired(TIMESTAMP + TIMEOUT)
    session.commandSessions.producers.size mustBe 1
    session.commandSessions.producers.contains(DOWNSTREAM_PRODUCER) mustBe true
    session.commandSessions.commandSession(DOWNSTREAM_PRODUCER).commands.size mustBe 0
  }

  //does not expire commandSession if producer is not alive but command list is non-empty
  @Test
  def doesNotExpireCommandSessionExpiredProducerNonEmptyCommands(): Unit = {
    val session = StatemachineSessions(TIMEOUT)
    session.addNewSession(CLIENT_ID, None, TIMESTAMP)
    session.updateSession(CLIENT_ID, 1, 0, ForwardingEmptyResponse(Set(DOWNSTREAM_ADDRESS)), TIMESTAMP + TIMEOUT - 1)
    session.removeExpired(TIMESTAMP + TIMEOUT + TIMEOUT)
    session.registeredClients.size mustBe 0
    session.commandSessions.producers.size mustBe 1
    session.commandSessions.producers.contains(DOWNSTREAM_PRODUCER) mustBe true
    session.commandSessions.commandSession(DOWNSTREAM_PRODUCER).commands.size mustBe 1
  }

  //expires commandSession if producer is not alive and command list is empty
  @Test
  def expiresCommandSessionExpiredProducerEmptyCommands(): Unit = {
    val session = StatemachineSessions(TIMEOUT)
    session.addNewSession(CLIENT_ID, None, TIMESTAMP)
    session.updateSession(CLIENT_ID, 1, 0, ForwardingEmptyResponse(Set(DOWNSTREAM_ADDRESS)), TIMESTAMP + TIMEOUT - 1)
    session.removeExpired(TIMESTAMP + TIMEOUT + TIMEOUT)
    session.registeredDownstreamClient(DOWNSTREAM_PRODUCER, DOWNSTREAM_REGISTERED_CLIENT.id)
    session.registeredClients.size mustBe 0
    session.removeCommands(DOWNSTREAM_REGISTERED_CLIENT, 2)
    session.commandSessions.producers.size mustBe 0
  }

  @Test
  def addsNewClientForSameDownstream(): Unit = {
    val client2 = "client_2"
    val client2Producer = RaftClusterProducer(DOWNSTREAM_PRODUCER.cluster, ClientProducer(client2, Seq()))
    val session = StatemachineSessions(TIMEOUT)
    session.addNewSession(CLIENT_ID, None, TIMESTAMP)
    session.updateSession(CLIENT_ID, 1, 0, ForwardingEmptyResponse(Set(DOWNSTREAM_ADDRESS)), TIMESTAMP + 1)
    session.addNewSession(client2, None, TIMESTAMP + 2)
    session.updateSession(client2, 1, 0, ForwardingEmptyResponse(Set(DOWNSTREAM_ADDRESS)), TIMESTAMP + 3)
    session.registeredClients.size mustBe 2
    session.registeredClients.contains(CLIENT_ID) mustBe true
    session.registeredClients.contains(client2) mustBe true
    session.commandSessions.producers.size mustBe 2
    session.commandSessions.producers.contains(DOWNSTREAM_PRODUCER) mustBe true
    session.commandSessions.producers.contains(client2Producer) mustBe true
    session.commandSessions.commandSession(DOWNSTREAM_PRODUCER).commands.size mustBe 1
    session.commandSessions.commandSession(client2Producer).commands.size mustBe 1
  }
}
