package zio.streams.push.internal

import zio.ZIO
import zio.streams.push.PushStream

trait SourcePushStream[A] extends PushStream[Any, Nothing, A] {
  override def subscribe[OutR2 <: Any, OutE2 >: Nothing](observer: Observer[OutR2, OutE2, A]): ZIO[OutR2, OutE2, Unit] = {
    zio.Console.printLine(s"SourcePushStream2 - subscribe").ignore *> startSource(observer).either.flatMap {
      case Left(error) => observer.onError(error)
      case Right(()) => observer.onComplete()
    }.onExit(exit => zio.Console.printLine(s"SourcePushStream - $exit").ignore)
  }

  protected def startSource[OutR2 <: Any, OutE2 >: Nothing](observer: Observer[OutR2, OutE2, A]): ZIO[OutR2, OutE2, Unit]

  protected def when[R, E](condition: Boolean)(zio: => ZIO[R, E, Unit]): ZIO[R, E, Unit] = if (condition) zio else ZIO.unit
}

trait SourcePushStream2[R, E, A] extends PushStream[R, E, A] {
  override def subscribe[OutR2 <: R, OutE2 >: E](observer: Observer[OutR2, OutE2, A]): ZIO[OutR2, OutE2, Unit] = {
    zio.Console.printLine(s"SourcePushStream2 - subscribe").ignore *> startSource(observer).either.flatMap {
      case Left(error) => zio.Console.printLine(s"SourcePushStream2 - onError before").ignore *> observer.onError(
          error
        ) *> zio.Console.printLine(s"SourcePushStream2 - onError after").ignore
      case Right(()) => zio.Console.printLine(
          s"SourcePushStream2 - onComplete before"
        ).ignore *> observer.onComplete() *> zio.Console.printLine(s"SourcePushStream2 - onComplete after").ignore
    }.onExit(exit => zio.Console.printLine(s"SourcePushStream2 - $exit").ignore)
  }

  protected def startSource[OutR2 <: R, OutE2 >: E](observer: Observer[OutR2, OutE2, A]): ZIO[OutR2, OutE2, Unit]

  protected def when[R, E](condition: Boolean)(zio: => ZIO[R, E, Unit]): ZIO[R, E, Unit] = if (condition) zio else ZIO.unit
}
