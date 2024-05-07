package zio.streams.push.internal.operators

import zio.streams.push.PushStream.Operator
import zio.streams.push.internal.Ack.Stop
import zio.streams.push.internal.{Ack, Observer}
import zio.{Ref, URIO, ZIO}

class TakeOperator[R, E, A](n: Long) extends Operator[A, R, E, A] {

  override def apply[OutR1 <: R, OutE1 >: E](out: Observer[OutR1, OutE1, A]): URIO[OutR1, Observer[OutR1, OutE1, A]] =
    Ref.make(0).map { ref =>
      new DefaultObserver[OutR1, OutE1, A](out) {
        override def onNext(elem: A): URIO[OutR1, Ack] = {
          ref.updateAndGet(_ + 1).flatMap { emitted =>
            out.onNext(elem).flatMap { downStreamAck =>
              if (emitted == n) {
                out.onComplete().as(Stop)
              } else ZIO.succeed(downStreamAck)
            }
          }
        }
      }
    }
}
