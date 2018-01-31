package rx.distributed.statemachine.commands

import rx.distributed.notifications.RxNotification
import rx.distributed.raft.statemachine.input.StatemachineCommand
import rx.lang.scala.Notification

@SerialVersionUID(53L)
case class RxEvent[T](streamId: String, notification: RxNotification[T]) extends StatemachineCommand

object RxEvent {
  def apply[T](streamId: String, notification: Notification[T]): RxEvent[T] = RxEvent(streamId, RxNotification(notification))
}
