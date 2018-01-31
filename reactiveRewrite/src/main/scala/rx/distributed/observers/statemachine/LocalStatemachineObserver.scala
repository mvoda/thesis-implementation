package rx.distributed.observers.statemachine

case class LocalStatemachineObserver(onNext: Any => Unit, onError: Throwable => Unit, onCompleted: () => Unit) extends StatemachineObserver

object LocalStatemachineObserver {
  def apply(onNext: Any => Unit): LocalStatemachineObserver = new LocalStatemachineObserver(onNext, _ => (), () => ())

  def apply(onNext: (Any) => Unit, onError: (Throwable) => Unit): LocalStatemachineObserver = new LocalStatemachineObserver(onNext, onError, () => ())

}
