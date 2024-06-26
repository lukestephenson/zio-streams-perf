package zio.streams.push.internal.operators

import zio.URIO
import zio.streams.push.internal.{Ack, Observer}

trait DefaultObserver[R, E, A](out: Observer[R, E, ?]) extends Observer[R, E, A] {
  override def onError(e: E): URIO[R, Unit] = out.onError(e)

  override def onComplete(): URIO[R, Unit] = out.onComplete()
}
