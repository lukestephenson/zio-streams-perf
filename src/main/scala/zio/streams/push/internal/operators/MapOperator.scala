package zio.streams.push.internal.operators

import zio.streams.push.PushStream.Operator
import zio.streams.push.internal.operators.DefaultObserver
import zio.streams.push.internal.{Ack, Observer}
import zio.{RIO, UIO, URIO, ZIO}

class MapOperator[R, E, A, B](f: A => B) extends Operator[A, R, E, B] {
  override def apply[OutR1 <: R](out: Observer[OutR1, E, B]): UIO[Observer[OutR1, E, A]] =
    ZIO.succeed(new DefaultObserver[OutR1, E, A](out) {
      override def onNext(elem: A): URIO[OutR1, Ack] = {
        out.onNext(f(elem))
      }
    })
}
