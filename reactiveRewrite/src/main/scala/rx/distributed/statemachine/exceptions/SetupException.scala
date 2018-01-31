package rx.distributed.statemachine.exceptions

case class SetupException(message: String) extends Exception(message)
