package zio.streams.push.internal.operators

import zio.streams.push.PushStream.Operator
import zio.streams.push.internal.Ack.Stop
import zio.streams.push.internal.{Ack, Observer}
import zio.{Ref, URIO, ZIO}

class TakeOperator[R, E, A](n: Long) extends Operator[A, R, E, A] {

  override def apply[OutR1 <: R](out: Observer[OutR1, E, A]): URIO[OutR1, Observer[OutR1, E, A]] =
    Ref.make(0).map { ref =>
      new DefaultObserver[OutR1, E, A](out) {
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
