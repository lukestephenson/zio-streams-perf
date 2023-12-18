package zio.streams.push.internal.operators

import zio.streams.push.PushStream.Operator
import zio.streams.push.internal.operators.DefaultObserver
import zio.streams.push.internal.{Ack, Observer}
import zio.{UIO, ZIO}

class MapZioOperator[InA, OutR, OutE, OutB](f: InA => ZIO[OutR, OutE, OutB]) extends Operator[InA, OutR, OutE, OutB] {
  def apply[OutR1 <: OutR, OutE1 >: OutE](out: Observer[OutR1, OutE1, OutB]): UIO[Observer[OutR1, OutE1, InA]] =
    ZIO.succeed(new DefaultObserver[OutR1, OutE1, InA](out) {
      override def onNext(elem: InA): ZIO[OutR1, OutE1, Ack] = {
        f(elem).flatMap(b => out.onNext(b))
      }
    })
}
