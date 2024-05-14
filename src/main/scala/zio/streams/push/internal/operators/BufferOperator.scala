package zio.streams.push.internal.operators
import zio.*
import zio.streams.push.PushStream.Operator
import zio.streams.push.internal.operators.BufferOperator.QueueType
import zio.streams.push.internal.{Ack, Acks, Observer}

object BufferOperator {
  enum QueueType[+OutE, +InA] {
    case QueueError(e: OutE)
    case QueueElement(elem: InA)
    case QueueComplete
  }

}

class BufferOperator[InA, OutR, OutE](queue: Queue[QueueType[OutE, InA]])
    extends Operator[InA, OutR, OutE, InA] {

  private enum Complete {
    case Done
  }

  def apply[OutR1 <: OutR](observer: Observer[OutR1, OutE, InA]): URIO[OutR1, Observer[OutR1, OutE, InA]] = {

    /** @param shutdownPromise
      *   This indicates that the queue has been drained after completion has been requested.
      * @return
      */
    def run(shutdownPromise: Promise[Nothing, Complete]): URIO[OutR1, Observer[OutR1, OutE, InA]] = {
      var stop = false
      var shutdown = false

      def debug(content: => String): UIO[Unit] = {
//        zio.Console.printLine(s"BufferOperator - $content").ignore
        ZIO.unit
      }

      val consumer = new Observer[OutR1, OutE, InA] {
        override def onNext(elem: InA): ZIO[OutR1, Nothing, Ack] = {
          if (stop) Acks.Stop
          else {
            for {
              _ <- debug(s"offer $elem")
              _ <- queue.offer(QueueType.QueueElement(elem))
            } yield if (stop) Ack.Stop else Ack.Continue
          }
        }

        override def onError(e: OutE): UIO[Unit] = offerAndWaitForShutdown(QueueType.QueueError(e))

        override def onComplete(): UIO[Unit] = offerAndWaitForShutdown(QueueType.QueueComplete)

        def offerAndWaitForShutdown(element: QueueType[OutE, InA]) = ZIO.unless(shutdown) {
          queue.offer(element) *> shutdownPromise.await
        }.unit
      }

      def take(): URIO[OutR1, Unit] = {
        val runOnce: URIO[OutR1, Ack] = for {
          _ <- debug("take")
          result <- queue.take
          _ <- debug(s"take got $result")
          lastAck <-
            result match
              case QueueType.QueueError(e) => debug("took a failure") *> observer.onError(e).as(Ack.Stop)
              case QueueType.QueueComplete => debug("took complete") *> observer.onComplete().as(Ack.Stop)
              case QueueType.QueueElement(element) => observer.onNext(element).tap(ack =>
                  debug("got stop from downstream") *> ZIO.when(ack == Ack.Stop)(ZIO.succeed {
                    stop = true
                    shutdown = true
                  } *> observer.onComplete().as(Ack.Stop) *> queue.takeAll // drain the queue in case it is full and the upstream consumer is blocked on submitting to the queue in onNext
                  )
                )
        } yield lastAck

        runOnce.flatMap {
          case Ack.Stop => ZIO.unit
          case Ack.Continue => take()
        }
      }

      for {
        takeFiber <- take().onError(cause => debug(s"take ended with $cause")).ensuring(
          shutdownPromise.succeed(Complete.Done)
        ).forkDaemon
      } yield consumer
    }

    // TODO these resources should ideally be scoped
    for {
      shutdownPromise <- Promise.make[Nothing, Complete]
      observer <- run(shutdownPromise)
    } yield observer
  }
}
