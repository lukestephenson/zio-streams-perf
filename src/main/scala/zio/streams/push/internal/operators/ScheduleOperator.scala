package zio.streams.push.internal.operators

import zio.streams.push.PushStream.Operator
import zio.streams.push.internal.{Ack, Observer, Observers}
import zio.{RIO, Schedule, UIO, URIO, ZIO}

class ScheduleOperator[InA, OutR, OutE, OutB, OutC](schedule: => Schedule[OutR, InA, OutB], f: InA => OutC, g: OutB => OutC)
    extends Operator[InA, OutR, OutE, OutC] {
  def apply[OutR1 <: OutR](out: Observer[OutR1, OutE, OutC]): UIO[Observer[OutR1, OutE, InA]] = {
    schedule.driver.map { driver =>
      new DefaultObserver[OutR1, OutE, InA](out) {
        override def onNext(elem: InA): URIO[OutR1, Ack] = {
          driver.next(elem).either.flatMap {
            case Left(None) =>
              out.onComplete().as(Ack.Stop)
            case Right(b) =>
              Observers.emitAll(out, Iterable(f(elem), g(b)))
          }
        }
      }
    }
  }
}
