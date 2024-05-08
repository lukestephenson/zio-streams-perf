package zio.streams.push.internal.operators

import zio.stm.TSemaphore
import zio.streams.push.PushStream.Operator
import zio.streams.push.internal.{Ack, Acks, Observer}
import zio.{Fiber, Promise, Queue, UIO, URIO, ZIO}

class MapParallelZioOperator[InA, OutR, OutE, OutB](parallelism: Int, f: InA => ZIO[OutR, OutE, OutB], name: String)
    extends Operator[InA, OutR, OutE, OutB] {

  private enum Complete {
    case Done
  }

  override def apply[OutR1 <: OutR](observer: Observer[OutR1, OutE, OutB]): URIO[OutR1, Observer[OutR1, OutE, InA]] = {

    /** @param permits
      *   The maximum number of tasks to run in parallel
      * @param buffer
      *   The queue used to decouple the upstream and downstream
      * @param completionPromise
      *   this can be completed or failed once to indicate there is a failure or completion communicate downstream. Either the upstream or
      *   downstream side of the queue may single completion or failure.
      * @param shutdownPromise
      *   This indicates that the queue has been drained after completion has been requested.
      * @return
      */
    def run(
        permits: TSemaphore,
        buffer: Queue[Fiber[OutE, OutB]],
        failurePromise: Promise[OutE, Complete],
        shutdownPromise: Promise[Nothing, Complete]): URIO[OutR1, Observer[OutR1, OutE, InA]] = {
      var stop = false

      def available(label: String): UIO[Unit] = {
        for {
          available <- permits.available.commit
          _ <- debug(s"$label - permits $available are available").ignore
        } yield ()
      }

      def debug(content: => String): UIO[Unit] = {
//        zio.Console.printLine(s"$name - $content").ignore
        ZIO.unit
      }

      val consumer = new Observer[OutR1, OutE, InA] {
        override def onNext(elem: InA): ZIO[OutR1, Nothing, Ack] = {
          if (stop) Acks.Stop
          else {
            for {
              _ <- debug("acquire permit")
              _ <- permits.acquire.commit
              _ <- debug("got a permit")
              fiber <- f(elem).onError { cause =>
                ZIO.when(cause.isInterrupted)(debug(s"onNext failed with $cause")) *>
                  failurePromise.failCause(cause) *> ZIO.succeed {
                    stop = true
                  }
              }.forkDaemon
              _ <- buffer.offer(fiber)
            } yield if (stop) Ack.Stop else Ack.Continue
          }
        }

        override def onError(e: OutE): UIO[Unit] = {
          // for errors, we want to interrupt any outstanding work immediately
          debug("onError start").ignore *> failurePromise.fail(e).unit *> shutdownPromise.await.unit *> debug("onError done").ignore
        }

        override def onComplete(): UIO[Unit] = {
          // for completion, we want to allow all outstanding fibers to complete, and then signal completion (but only if
          // obsever.onError hasn't been called already).
          (debug(s"onComplete start - waiting for $parallelism permits") *>
            available("onComplete") *>
            ZIO.raceFirst(permits.acquireN(parallelism).commit, List(failurePromise.await.ignore)) *> debug(
              "onComplete got permits or already failed"
            ) *>
            failurePromise.succeed(Complete.Done).unit *> shutdownPromise.await.unit *> debug(" onComplete done")).onExit(exit =>
            debug(s"oncomplete exit with $exit")
          )
        }
      }

      def cancelRunningJobs(): UIO[Unit] = {
        for {
          fibers <- buffer.takeAll
          _ <- debug(s"cancelRunningJobs - ${fibers.size} fibers to cancel")
          _ <- ZIO.foreachDiscard(fibers)(_.interrupt)
          _ <- available("pre release")
          _ <- permits.releaseN(parallelism).commit
          _ <- available("post release 1")
          _ <- available("post release 2")
        } yield ()
      }

      def take(): URIO[OutR1, Nothing] = {
        val runOnce: URIO[OutR1, Unit] = for {
          fiber <- buffer.take
          _ <-
            fiber.join.foldCauseZIO(
              cause =>
                ZIO.when(cause.isInterrupted)(debug(s"fiber.join failed with $cause")) *>
                  failurePromise.failCause(cause),
              success = fiberResult =>
                observer.onNext(fiberResult).map(ack => if (ack == Ack.Stop) stop = true else ()).unit
            )
              .onInterrupt(debug(s" cancel fiber from take") *> fiber.interrupt)
          _ <- permits.release.commit
        } yield ()

        runOnce *> take()
      }

      for {
        takeFiber <- take().onError(cause => debug(s"take ended with $cause")).forkDaemon
        // Only call onError once on downstream. TODO, handle other failure types
        _ <- failurePromise.await
          .foldCauseZIO(
            failure = { cause =>
              debug(s"failed with $cause") *>
                ZIO.succeed {
                  stop = true
                } *>
                cause.failureOption.fold(observer.onError(null.asInstanceOf[OutE]))(e => observer.onError(e)) *>
                cancelRunningJobs()
            },
            success = { _ => observer.onComplete() }
          )
          .ensuring(available("pre interrupt") *> takeFiber.interrupt.flatMap(exit => debug(s"interrupt finished with $exit")) *> available(
            "post interrupt"
          ))
          .ensuring(shutdownPromise.succeed(Complete.Done))
          .fork
      } yield consumer

    }

    // TODO these resources should ideally be scoped
    for {
      permits <- TSemaphore.makeCommit(parallelism)
      buffer <- Queue.bounded[Fiber[OutE, OutB]](parallelism * 2)
      failurePromise <- Promise.make[OutE, Complete]
      shutdownPromise <- Promise.make[Nothing, Complete]
      observer <- run(permits, buffer, failurePromise, shutdownPromise)
    } yield observer
  }
}
