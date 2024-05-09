package zio.streams.push.internal

import zio.streams.push.PushStream
import zio.{URIO, ZIO}

trait SourcePushStream[R, E, A] extends PushStream[R, E, A] {
  override def subscribe[OutR2 <: R](observer: Observer[OutR2, E, A]): URIO[OutR2, Unit] = {
    startSource(observer).flatMap(_ => observer.onComplete())
  }

  protected def startSource[OutR2 <: R](observer: Observer[OutR2, E, A]): URIO[OutR2, Unit]

  protected def when[R, E](condition: Boolean)(zio: => ZIO[R, E, Unit]): ZIO[R, E, Unit] = if (condition) zio else ZIO.unit
}
