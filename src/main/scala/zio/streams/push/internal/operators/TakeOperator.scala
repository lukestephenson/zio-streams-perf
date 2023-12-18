package zio.streams.push.internal.operators

import zio.streams.push.PushStream.Operator
import zio.streams.push.internal.Ack.Stop
import zio.streams.push.internal.{Ack, Observer}
import zio.{Ref, ZIO}

class TakeOperator[R, E, A](n: Long) extends Operator[A, R, E, A] {

  override def apply[OutR1 <: R, OutE1 >: E](out: Observer[OutR1, OutE1, A]): ZIO[OutR1, OutE1, Observer[OutR1, OutE1, A]] =
    Ref.make(0).map { ref =>
      new DefaultObserver[OutR1, OutE1, A](out) {
        override def onNext(elem: A): ZIO[OutR1, OutE1, Ack] = {
          ref.updateAndGet(i => i + 1).flatMap(emitted => if (emitted > n) out.onComplete().as(Stop) else out.onNext(elem))
        }
      }
    }
}
