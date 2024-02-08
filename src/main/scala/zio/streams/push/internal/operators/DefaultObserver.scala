package zio.streams.push.internal.operators

import zio.ZIO
import zio.streams.push.internal.Observer

trait DefaultObserver[R, E, A](out: Observer[R, E, _]) extends Observer[R, E, A] {
  override def onError(e: E): ZIO[R, E, Unit] = out.onError(e)

  override def onComplete(): ZIO[R, E, Unit] = out.onComplete()
}
