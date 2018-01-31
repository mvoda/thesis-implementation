package rx.distributed.observers.remote

import rx.distributed.observers.statemachine.StatemachineObserver
import rx.distributed.operators.Operator
import rx.distributed.operators.combining.CombiningOperator
import rx.distributed.operators.single.SingleOperator

case class StreamSetupObserver[T](operators: Seq[Operator], statemachineObserver: StatemachineObserver) {
  def addOperator[S](operator: SingleOperator[S, T]): StreamSetupObserver[S] = StreamSetupObserver(operators :+ operator, statemachineObserver)

  def addOperator[S, R](operator: CombiningOperator[S, R, T]): StreamSetupObserver[S] =
    StreamSetupObserver(operators :+ operator, statemachineObserver)

  def addOperatorLeft[Left, Right](operator: CombiningOperator[Left, Right, T]): StreamSetupObserver[Left] =
    StreamSetupObserver(operators :+ operator, statemachineObserver)

  def addOperatorRight[Left, Right](operator: CombiningOperator[Left, Right, T]): StreamSetupObserver[Right] =
    StreamSetupObserver(operators :+ operator, statemachineObserver)
}
