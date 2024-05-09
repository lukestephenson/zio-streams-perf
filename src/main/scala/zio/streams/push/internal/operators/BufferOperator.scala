package zio.streams.push.internal.operators
import zio.streams.push.PushStream.Operator
import zio.streams.push.internal.{Ack, Acks, Observer}
import zio.*

class BufferOperator[InA, OutR, OutE](queue: Queue[Either[OutE, InA]])
    extends Operator[InA, OutR, OutE, InA] {

  private type QueueType = Either[OutE, InA]

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
        queue: Queue[QueueType],
        failurePromise: Promise[Nothing, OutE],
        completionPromise: Promise[Nothing, Complete],
        shutdownPromise: Promise[Nothing, Complete]): URIO[OutR1, Observer[OutR1, OutE, InA]] = {
      var stop = false

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
              _ <- queue.offer(Right(elem))
            } yield if (stop) Ack.Stop else Ack.Continue
          }
        }

        override def onError(e: OutE): UIO[Unit] = {
          // for errors, we want to interrupt any outstanding work immediately
          debug("onError start").ignore *> queue.offer(Left(e)) *> shutdownPromise.await.unit *> debug("onError done").ignore
        }

        override def onComplete(): UIO[Unit] = {
          completionPromise.succeed(Complete.Done).unit *> shutdownPromise.await.unit *> debug(" onComplete done").onExit(exit =>
            debug(s"oncomplete exit with $exit")
          )
        }
      }

      def take(): URIO[OutR1, Unit] = {
        val runOnce: URIO[OutR1, Ack] = for {
          _ <- debug("take")
          result <- queue.take.raceEither(completionPromise.await.disconnect)
          _ <- debug(s"take got $result")
          lastAck <-
            result match
              case Left(element) => pushDownstream(element)
              case Right(Complete.Done) =>
                ZIO.succeed(Ack.Stop)
        } yield lastAck

        runOnce.flatMap {
          case Ack.Stop => ZIO.unit
          case Ack.Continue => take()
        }
      }

      def pushDownstream(element: QueueType): URIO[OutR1, Ack] = {
        if (stop) {
          ZIO.succeed(Ack.Stop)
        } else {
          element match
            case Left(value) => debug("took a failure") *> failurePromise.succeed(value).as(Ack.Stop)
            case Right(value) => observer.onNext(value).tap(ack =>
                ZIO.when(ack == Ack.Stop)(ZIO.succeed {
                  stop = true
                } *> completionPromise.succeed(Complete.Done))
              )
        }
      }

      def finishRemainingElementsAndComplete(takeFiber: Fiber[Nothing, Unit]): URIO[OutR1, Unit] = {
        for {
          _ <- completionPromise.await
          _ <- debug("finishRemainingElementsAndComplete")
          _ <- takeFiber.join.ignore
          remainingElements <- queue.takeAll
          _ <- ZIO.foreachDiscard(remainingElements)(pushDownstream)
          _ <- observer.onComplete() // TODO an error may have occurred emitting the remaining elements and onComplete should not be called.
        } yield ()
      }

      // Only call onError once on downstream. TODO, handle other failure types
      def failureHandling(takeFiber: Fiber[Nothing, Unit]) = failurePromise.await
        .ensuring(takeFiber.interrupt.flatMap(exit => debug(s"interrupt finished with $exit")))
        .foldCauseZIO(
          failure = { cause =>
            debug(s"failed with $cause") *>
              ZIO.succeed {
                stop = true
              } *>
              cause.failureOption.fold(observer.onError(null.asInstanceOf[OutE]))(e => observer.onError(e))
          },
          success = { error => observer.onError(error) }
        )

      for {
        takeFiber <- take().onError(cause => debug(s"take ended with $cause")).forkDaemon
        _ <- ZIO.raceFirst(finishRemainingElementsAndComplete(takeFiber), List(failureHandling(takeFiber))).ensuring(
          shutdownPromise.succeed(Complete.Done)
        ).fork
      } yield consumer
    }

    // TODO these resources should ideally be scoped
    for {
      failurePromise <- Promise.make[Nothing, OutE]
      completionPromise <- Promise.make[Nothing, Complete]
      shutdownPromise <- Promise.make[Nothing, Complete]
      observer <- run(queue, failurePromise, completionPromise, shutdownPromise)
    } yield observer
  }
}
