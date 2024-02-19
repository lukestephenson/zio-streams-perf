package zio.streams.push.internal

import zio.{UIO, ZIO}

trait Observer[R, E, -T] {
  def onNext(elem: T): ZIO[R, E, Ack]

  // TODO remove E from the result type here. Confusing that errors go in two directions
  def onError(e: E): ZIO[R, E, Unit]

  def onComplete(): ZIO[R, E, Unit]
}

sealed abstract class Ack {}

object Acks {
  val Stop: UIO[Ack] = ZIO.succeed(Ack.Stop)
  val Continue: UIO[Ack] = ZIO.succeed(Ack.Continue)
}

object Ack {
  case object Stop extends Ack

  case object Continue extends Ack
}
