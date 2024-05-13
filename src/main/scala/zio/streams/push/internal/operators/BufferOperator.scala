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

    /** @param completionPromise
      *   this can be completed or failed once to indicate there is a failure or completion communicate downstream. Either the upstream or
      *   downstream side of the queue may single completion or failure.
      * @param shutdownPromise
      *   This indicates that the queue has been drained after completion has been requested.
      * @return
      */
    def run(
        failurePromise: Promise[Nothing, OutE],
        shutdownPromise: Promise[Nothing, Complete]): URIO[OutR1, Observer[OutR1, OutE, InA]] = {
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

        override def onError(e: OutE): UIO[Unit] = {
          // for errors, we want to interrupt any outstanding work immediately
          ZIO.unless(shutdown) {
            debug("onError start").ignore *> queue.offer(QueueType.QueueError(e)) *> shutdownPromise.await.unit *> debug(
              "onError done"
            ).ignore
          }.unit
        }

        override def onComplete(): UIO[Unit] = {
          ZIO.unless(shutdown) {
            queue.offer(QueueType.QueueComplete) *> shutdownPromise.await.unit *> debug(" onComplete done").onExit(exit =>
              debug(s"oncomplete exit with $exit")
            )
          }.unit
        }
      }

      def take(): URIO[OutR1, Unit] = {
        val runOnce: URIO[OutR1, Ack] = for {
          _ <- debug("take")
          result <- queue.take
          _ <- debug(s"take got $result")
          lastAck <-
            result match
              case QueueType.QueueError(e) => debug("took a failure") *> failurePromise.succeed(e).as(Ack.Stop)
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

      // Only call onError once on downstream. TODO, handle other failure types
      def failureHandling(takeFiber: Fiber[Nothing, Unit]) = failurePromise.await
        .ensuring(takeFiber.interrupt.flatMap(exit => debug(s"interrupt finished with $exit")))
        .foldCauseZIO(
          failure = { cause =>
            debug(s"failed with $cause") *>
              ZIO.succeed {
                stop = true
                shutdown = true
              } *>
              cause.failureOption.fold(observer.onError(null.asInstanceOf[OutE]))(e => observer.onError(e))
          },
          success = { error => observer.onError(error) }
        )

      for {
        takeFiber <- take().onError(cause => debug(s"take ended with $cause")).ensuring(
          shutdownPromise.succeed(Complete.Done)
        ).forkDaemon
        _ <- failureHandling(takeFiber).ensuring(
          shutdownPromise.succeed(Complete.Done)
        ).fork
      } yield consumer
    }

    // TODO these resources should ideally be scoped
    for {
      failurePromise <- Promise.make[Nothing, OutE]
      shutdownPromise <- Promise.make[Nothing, Complete]
      observer <- run(failurePromise, shutdownPromise)
    } yield observer
  }
}
