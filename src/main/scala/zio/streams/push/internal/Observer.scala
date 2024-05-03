package zio.streams.push.internal

import zio.{UIO, URIO, ZIO}

trait Observer[R, E, -T] {
  def onNext(elem: T): URIO[R, Ack]

  def onError(e: E): URIO[R, Unit]

  def onComplete(): URIO[R, Unit]
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
