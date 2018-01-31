package rx.distributed.raft.statemachine.session

import org.apache.logging.log4j.scala.Logging
import org.junit.Test
import org.scalatest.MustMatchers._
import rx.distributed.raft.client.RaftClusterClient
import rx.distributed.raft.consensus.server.Address.{ServerAddress, TestingAddress}
import rx.distributed.raft.statemachine.input.DownstreamCommand
import rx.distributed.raft.statemachine.output.StatemachineOutputUtils.EmptyCommand

class CommandSessionsTest extends Logging {
  private val CLIENT_1 = "client_1"
  private val CLIENT_2 = "client_2"
  private val ADDRESS_1 = TestingAddress("address_1")
  private val ADDRESS_2 = TestingAddress("address_2")
  private val CLIENT_PRODUCER_1 = ClientProducer(CLIENT_1, Seq())
  private val CLIENT_PRODUCER_2 = ClientProducer(CLIENT_2, Seq())
  private val CLUSTER_PRODUCER_1_1 = RaftClusterProducer(Set(ADDRESS_1), CLIENT_PRODUCER_1)
  private val CLUSTER_PRODUCER_1_2 = RaftClusterProducer(Set(ADDRESS_2), CLIENT_PRODUCER_1)
  private val CLUSTER_PRODUCER_2_1 = RaftClusterProducer(Set(ADDRESS_1), CLIENT_PRODUCER_2)

  @Test
  def createsNewProducerForSameSourceDifferentDestination(): Unit = {
    val sessions = CommandSessions()
    sessions.update(CLIENT_PRODUCER_1, Seq(forwardingEmptyCommand(ADDRESS_1)))
    sessions.update(CLIENT_PRODUCER_1, Seq(forwardingEmptyCommand(ADDRESS_2)))
    sessions.producers.size mustBe 2
  }

  @Test
  def doesNotCreateNewProducerForSameSourceAndDestination(): Unit = {
    val sessions = CommandSessions()
    sessions.update(CLIENT_PRODUCER_1, Seq(forwardingEmptyCommand(ADDRESS_1)))
    sessions.update(CLIENT_PRODUCER_1, Seq(forwardingEmptyCommand(ADDRESS_1)))
    sessions.producers.size mustBe 1
  }

  @Test
  def createsNewProducerForDifferentSourceSameDestination(): Unit = {
    val sessions = CommandSessions()
    sessions.update(CLIENT_PRODUCER_1, Seq(forwardingEmptyCommand(ADDRESS_1)))
    sessions.update(CLIENT_PRODUCER_2, Seq(forwardingEmptyCommand(ADDRESS_1)))
    sessions.producers.size mustBe 2
  }

  @Test
  def registersCorrectClient(): Unit = {
    val sessions = CommandSessions()
    sessions.update(CLIENT_PRODUCER_1, Seq(forwardingEmptyCommand(ADDRESS_1)))
    sessions.update(CLIENT_PRODUCER_1, Seq(forwardingEmptyCommand(ADDRESS_2)))
    sessions.update(CLIENT_PRODUCER_2, Seq(forwardingEmptyCommand(ADDRESS_1)))
    sessions.registeredDownstreamClient(CLUSTER_PRODUCER_1_2, "1")
    sessions.registeredDownstreamClient(CLUSTER_PRODUCER_1_1, "1")
    sessions.registeredDownstreamClient(CLUSTER_PRODUCER_2_1, "test")
    sessions.registeredDownstreamClientSessions().size mustBe 3
    sessions.commandSession(CLUSTER_PRODUCER_1_1).clientId mustBe Some("1")
    sessions.commandSession(CLUSTER_PRODUCER_1_2).clientId mustBe Some("1")
    sessions.commandSession(CLUSTER_PRODUCER_2_1).clientId mustBe Some("test")
  }

  @Test
  def removeCommandFromCorrectClient(): Unit = {
    val sessions = CommandSessions()
    sessions.update(CLIENT_PRODUCER_1, Seq(forwardingEmptyCommand(ADDRESS_1)))
    sessions.update(CLIENT_PRODUCER_1, Seq(forwardingEmptyCommand(ADDRESS_2)))
    sessions.registeredDownstreamClient(CLUSTER_PRODUCER_1_2, "1")
    sessions.registeredDownstreamClient(CLUSTER_PRODUCER_1_1, "1")
    sessions.registeredDownstreamClientSessions().size mustBe 2
    sessions.commandSession(CLUSTER_PRODUCER_1_1).clientId mustBe Some("1")
    sessions.commandSession(CLUSTER_PRODUCER_1_2).clientId mustBe Some("1")
    sessions.removeCommands(RaftClusterClient(Set(ADDRESS_1), "1"), 1)
    sessions.commandSession(CLUSTER_PRODUCER_1_1).commands.size mustBe 0
    sessions.commandSession(CLUSTER_PRODUCER_1_2).commands.size mustBe 1
  }

  @Test
  def correctlyRemovesExpiredSessionOnRemovalThenExpiry(): Unit = {
    val sessions = CommandSessions()
    sessions.update(CLIENT_PRODUCER_1, Seq(forwardingEmptyCommand(ADDRESS_1)))
    sessions.update(CLIENT_PRODUCER_1, Seq(forwardingEmptyCommand(ADDRESS_2)))
    sessions.registeredDownstreamClient(CLUSTER_PRODUCER_1_2, "1")
    sessions.registeredDownstreamClient(CLUSTER_PRODUCER_1_1, "1")
    sessions.removeCommands(RaftClusterClient(Set(ADDRESS_1), "1"), 1)
    sessions.producers.size mustBe 2
    sessions.registeredDownstreamClientSessions().size mustBe 2
    val expiredClients = sessions.removeExpiredSessions(Set(CLIENT_PRODUCER_1.clientId))
    expiredClients.size mustBe 1
    expiredClients.head.id mustBe "1"
    expiredClients.head.cluster mustBe Set(ADDRESS_1)
    sessions.producers.size mustBe 1
    sessions.producers.head mustBe CLUSTER_PRODUCER_1_2
  }

  @Test
  def correctlyRemovesExpiredSessionOnExpiryThenRemoval(): Unit = {
    val sessions = CommandSessions()
    sessions.update(CLIENT_PRODUCER_1, Seq(forwardingEmptyCommand(ADDRESS_1)))
    sessions.update(CLIENT_PRODUCER_1, Seq(forwardingEmptyCommand(ADDRESS_2)))
    sessions.registeredDownstreamClient(CLUSTER_PRODUCER_1_2, "1")
    sessions.registeredDownstreamClient(CLUSTER_PRODUCER_1_1, "1")
    val expiredClients = sessions.removeExpiredSessions(Set(CLIENT_PRODUCER_1.clientId))
    sessions.producers.size mustBe 2
    sessions.registeredDownstreamClientSessions().size mustBe 2
    expiredClients.size mustBe 0
    val maybeExpiredClient = sessions.removeCommands(RaftClusterClient(Set(ADDRESS_1), "1"), 1)
    maybeExpiredClient mustBe Some(RaftClusterClient(Set(ADDRESS_1), "1"))
    sessions.producers.size mustBe 1
    sessions.producers.head mustBe CLUSTER_PRODUCER_1_2
  }

  private def forwardingEmptyCommand(address: ServerAddress): DownstreamCommand =
    DownstreamCommand(Set(address), EmptyCommand())

}
