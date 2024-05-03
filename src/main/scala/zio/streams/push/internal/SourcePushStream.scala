package zio.streams.push.internal

import zio.streams.push.PushStream
import zio.{URIO, ZIO}

trait SourcePushStream[R, E, A] extends PushStream[R, E, A] {
  override def subscribe[OutR2 <: R, OutE2 >: E](observer: Observer[OutR2, OutE2, A]): URIO[OutR2, Unit] = {
    startSource(observer).either.flatMap {
      case Left(error) => observer.onError(error)
      case Right(()) => observer.onComplete()
    }
  }

  protected def startSource[OutR2 <: R, OutE2 >: E](observer: Observer[OutR2, OutE2, A]): ZIO[OutR2, OutE2, Unit]

  protected def when[R, E](condition: Boolean)(zio: => ZIO[R, E, Unit]): ZIO[R, E, Unit] = if (condition) zio else ZIO.unit
}
