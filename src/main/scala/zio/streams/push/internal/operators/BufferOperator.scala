package zio.streams.push.internal.operators

import zio.streams.push.PushStream.Operator
import zio.streams.push.internal.{Ack, Observer, Observers}
import zio.{Promise, Queue, RIO, Schedule, UIO, URIO, ZIO}

class BufferOperator[InA, OutR, OutE](queue: => Queue[InA])
    extends Operator[InA, OutR, OutE, InA] {
  def apply[OutR1 <: OutR, OutE1 >: OutE](out: Observer[OutR1, OutE1, InA]): URIO[OutR1, Observer[OutR1, OutE1, InA]] = {
    val observer = new DefaultObserver[OutR1, OutE1, InA](out) {
      override def onNext(elem: InA): URIO[OutR1, Ack] = {
        queue.offer(elem).as(Ack.Continue)
//          .flatMap(accepted => zio.Console.printLine(s"$elem - accepted $accepted").ignore)
      }

      override def onComplete(): URIO[OutR1, Unit] = {
        // TODO Ideally flush out the last received element for a sliding queue (even though ZStream doesn't do this)
        super.onComplete()
      }
    }

    def queueProcessor(): URIO[OutR1, Ack] = {
      queue.take.flatMap((elem: InA) => Observers.emit(out, elem, queueProcessor()))
    }

    val x: ZIO[OutR1, Nothing, DefaultObserver[OutR1, OutE1, InA]] = queueProcessor().fork.as(observer)
    x
  }
}
