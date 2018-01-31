package rx.distributed.statemachine.exceptions

case class StatemachineInputException(message: String) extends Exception(message)
