package zio.streams.push.internal.operators

import zio.streams.push.PushStream.Operator
import zio.streams.push.internal.{Ack, Observer}
import zio.{Ref, UIO, URIO, ZIO}

class MapZioOperator[InA, OutR, OutE, OutB](f: InA => ZIO[OutR, OutE, OutB]) extends Operator[InA, OutR, OutE, OutB] {
  def apply[OutR1 <: OutR](out: Observer[OutR1, OutE, OutB]): UIO[Observer[OutR1, OutE, InA]] =
    for {
      errorSent <- Ref.make(false)
    } yield new DefaultObserver[OutR1, OutE, InA](out) {
      override def onNext(elem: InA): URIO[OutR1, Ack] = {
        f(elem).foldZIO(e => errorSent.set(true) *> out.onError(e).as(Ack.Stop), out.onNext)
      }

      override def onComplete(): URIO[OutR1, Unit] = {
        ZIO.unlessZIO(errorSent.get)(super.onComplete()).unit
      }
    }
}
