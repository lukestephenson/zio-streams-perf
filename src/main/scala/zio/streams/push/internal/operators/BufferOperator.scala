package zio.streams.push.internal.operators
import zio.streams.push.PushStream.Operator
import zio.streams.push.internal.{Ack, Acks, Observer}
import zio.*

enum BufferStrategy {
  case Blocking(capacity: Int)
  case BufferThenDropOldest(capacity: Int)
}

class BufferOperator[InA, OutR, OutE](bufferStrategy: BufferStrategy)
    extends Operator[InA, OutR, OutE, InA] {

  private enum Complete {
    case Done
  }

  def apply[OutR1 <: OutR, OutE1 >: OutE](observer: Observer[OutR1, OutE1, InA]): URIO[OutR1, Observer[OutR1, OutE1, InA]] = {

    type QueueType = Either[OutE1, InA]

    /** @param completionPromise
      *   this can be completed or failed once to indicate there is a failure or completion communicate downstream. Either the upstream or
      *   downstream side of the queue may single completion or failure.
      * @param shutdownPromise
      *   This indicates that the queue has been drained after completion has been requested.
      * @return
      */
    def run(
        queue: Queue[QueueType],
        failurePromise: Promise[Nothing, OutE1],
        completionPromise: Promise[Nothing, Complete],
        shutdownPromise: Promise[Nothing, Complete]): URIO[OutR1, Observer[OutR1, OutE1, InA]] = {
      var stop = false

      def debug(content: => String): UIO[Unit] = {
        //        zio.Console.printLine(s"$name - $content").ignore
        ZIO.unit
      }

      val consumer = new Observer[OutR1, OutE1, InA] {
        override def onNext(elem: InA): ZIO[OutR1, Nothing, Ack] = {
          if (stop) Acks.Stop
          else {
            for {
              _ <- debug(s"offer $elem")
              _ <- queue.offer(Right(elem))
            } yield if (stop) Ack.Stop else Ack.Continue
          }
        }

        override def onError(e: OutE1): UIO[Unit] = {
          // for errors, we want to interrupt any outstanding work immediately
          debug("onError start").ignore *> queue.offer(Left(e)) *> shutdownPromise.await.unit *> debug("onError done").ignore
        }

        override def onComplete(): UIO[Unit] = {
          completionPromise.succeed(Complete.Done).unit *> shutdownPromise.await.unit *> debug(" onComplete done").onExit(exit =>
            debug(s"oncomplete exit with $exit")
          )
        }
      }

      def take(): URIO[OutR1, Nothing] = {
        val runOnce: URIO[OutR1, Unit] = for {
          element <- queue.take
          _ <- ZIO.unless(stop) {
            element match
              case Left(value) => observer.onError(value)
              case Right(value) => observer.onNext(value).flatMap(ack =>
                  ZIO.when(ack == Ack.Stop)(ZIO.succeed { stop = true } *> completionPromise.succeed(Complete.Done))
                )
          }
        } yield ()

        runOnce *> take()
      }

      def waitUntilEmpty(): UIO[Unit] =
        debug("waitUntilEmpty") *> queue.size.flatMap(size => if (size == 0) ZIO.unit else ZIO.sleep(10.millis) *> waitUntilEmpty())

      val gracefulCompletion = completionPromise.await.flatMap(Complete => waitUntilEmpty())
      def failureHandling(takeFiber: Fiber[Nothing, Nothing]) = failurePromise.await
        .foldCauseZIO(
          failure = { cause =>
            debug(s"failed with $cause") *>
              ZIO.succeed {
                stop = true
              } *>
              cause.failureOption.fold(observer.onError(null.asInstanceOf[OutE1]))(e => observer.onError(e))
          },
          success = { _ => observer.onComplete() }
        )
        .ensuring(takeFiber.interrupt.flatMap(exit => debug(s"interrupt finished with $exit")))

      for {
        takeFiber <- take().onError(cause => debug(s"take ended with $cause")).forkDaemon
        // Only call onError once on downstream. TODO, handle other failure types
        _ <- ZIO.raceFirst(gracefulCompletion, List(failureHandling(takeFiber))).ensuring(shutdownPromise.succeed(Complete.Done)).fork
      } yield consumer
    }

    def makeQueue: UIO[Queue[QueueType]] = {
      bufferStrategy match
        case BufferStrategy.Blocking(capacity) => Queue.bounded[QueueType](capacity)
        case BufferStrategy.BufferThenDropOldest(capacity) => Queue.sliding[QueueType](capacity)
    }

    // TODO these resources should ideally be scoped
    for {
      queue <- makeQueue
      failurePromise <- Promise.make[Nothing, OutE1]
      completionPromise <- Promise.make[Nothing, Complete]
      shutdownPromise <- Promise.make[Nothing, Complete]
      observer <- run(queue, failurePromise, completionPromise, shutdownPromise)
    } yield observer
  }
}
