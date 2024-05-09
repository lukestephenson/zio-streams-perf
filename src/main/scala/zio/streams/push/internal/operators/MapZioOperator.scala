package zio.streams.push.internal.operators

import zio.streams.push.PushStream.Operator
import zio.streams.push.internal.{Ack, Observer}
import zio.{RIO, UIO, URIO, ZIO}

class MapZioOperator[InA, OutR, OutE, OutB](f: InA => ZIO[OutR, OutE, OutB]) extends Operator[InA, OutR, OutE, OutB] {
  def apply[OutR1 <: OutR](out: Observer[OutR1, OutE, OutB]): UIO[Observer[OutR1, OutE, InA]] =
    ZIO.succeed(new DefaultObserver[OutR1, OutE, InA](out) {
      override def onNext(elem: InA): URIO[OutR1, Ack] = {
        f(elem).foldZIO(e => out.onError(e).as(Ack.Stop), out.onNext)
      }
    })
}
