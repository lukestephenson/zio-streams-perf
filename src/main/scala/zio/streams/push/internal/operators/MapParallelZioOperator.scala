package zio.streams.push.internal.operators

import zio.stm.TSemaphore
import zio.streams.push.PushStream.Operator
import zio.streams.push.internal.{Ack, Acks, Observer}
import zio.{Fiber, Promise, Queue, ZIO}

class MapParallelZioOperator[InA, OutR, OutE, OutB](parallelism: Int, f: InA => ZIO[OutR, OutE, OutB])
    extends Operator[InA, OutR, OutE, OutB] {

  private enum Complete {
    case Done
  }

  override def apply[OutR1 <: OutR, OutE1 >: OutE](observer: Observer[OutR1, OutE1, OutB])
      : ZIO[OutR1, OutE1, Observer[OutR1, OutE1, InA]] = {
    TSemaphore.makeCommit(parallelism).flatMap { permits =>
      Queue.bounded[Fiber[OutE1, OutB]](parallelism * 2).flatMap { buffer =>
//        Promise.make[OutE1, Complete].flatMap { completionPromise =>
        Promise.make[OutE1, Complete].flatMap { failurePromise =>
          var stop = false

          def cancelRunningJobs(): ZIO[Any, Nothing, Unit] = zio.Console.printLine(s"cancel running jobs ignored").either.unit
          //            for {
          //              fibers <- buffer.takeAll
          //              _ <- zio.Console.printLine(s"size ${fibers.size} cancelling fibers").either
          //              _ <- ZIO.foreachDiscard(fibers)(f => zio.Console.printLine(s"size ${fibers.size} - about to interrupt").either *> f.interrupt *> zio.Console.printLine(s"size ${fibers.size} - done interrupt").either)
          //              _ <- zio.Console.printLine(s"size ${fibers.size} done cancelling fibers").either
          //              _ <- buffer.shutdown
          //            } yield ()

          val consumer = new Observer[OutR1, OutE1, InA] {
            override def onNext(elem: InA): ZIO[OutR1, Nothing, Ack] = {
              if (stop) Acks.Stop
              else {
                for {
                  _ <- zio.Console.printLine("acquire permit").either
                  _ <- permits.acquire.commit
                  fiber <- f(elem).onError { cause =>
                    zio.Console.printLine(s"picked up a failure in onNext").either *> failurePromise.failCause(cause) *> ZIO.succeed {
                      stop = true
                    } *> cancelRunningJobs()
                  }.fork
                  _ <- zio.Console.printLine("add fiber to buffer").either *> buffer.offer(fiber)
                } yield Ack.Continue
              }
            }

            override def onError(e: OutE1): ZIO[OutR1, OutE1, Unit] = {
              // for errors, we want to interrupt any outstanding work immediately
              zio.Console.printLine("error shutdown started").ignore *> failurePromise.fail(e) *> zio.Console.printLine(
                "error shutdown completed"
              ).ignore
            }

            override def onComplete(): ZIO[OutR1, OutE1, Unit] = {
              // for completion, we want to allow all outstanding fibers to complete, and then signal completion (but only if
              // obsever.onError hasn't been called already)
              zio.Console.printLine("onComplete shutdown started").ignore *>
                permits.acquireN(parallelism).commit *>
                zio.Console.printLine("onComplete everything is done - time to signal").ignore *>
                observer.onComplete()
            }
          }

          def take(): ZIO[OutR1, OutE1, Nothing] = {
            val runOnce = for {
              fiber <- buffer.take // TODO
              _ <- zio.Console.printLine(s"took from buffer").either
              _ <- ZIO.raceFirst(fiber.join, List(failurePromise.await)).foldCauseZIO(
                failure = { cause =>
                  val errorSteps: ZIO[OutR1, Nothing, Unit] = zio.Console.printLine(
                    s"picked up a failure in take ${cause.failureOption}"
                  ).either *> failurePromise.failCause(cause) *> fiber.interrupt.unit // TODO remove the either hack
                  errorSteps
                },
                success = { fiberResult =>
                  if (fiberResult == Complete)
                    zio.Console.printLine(
                      "should never reach this point"
                    ).either *> ZIO.dieMessage("should never reach this point")
                  else
                    observer.onNext(fiberResult.asInstanceOf[OutB]).map(ack => if (ack == Ack.Stop) stop = true else ()).unit
                }
              )
              _ <- zio.Console.printLine(s"releasing permit").either *> permits.release.commit
            } yield ()

            runOnce *> take()
          }

          for {
            takeFiber <- take().onExit(exit => zio.Console.printLine(s"mapParallelOperator take has finished with $exit").ignore).fork
            // Only call onError once on downstream. TODO, handle other failure types
            _ <- failurePromise.await.foldCauseZIO(
              failure = { cause =>
                zio.Console.printLine(s"failure promise has completed").ignore *> ZIO.succeed {
                  stop = true
                } *> ZIO.foreachDiscard(
                  cause.failureOption
                )(e => observer.onError(e).either)
              },
              success = { _ => zio.Console.printLine(s"everything is completed").ignore *> observer.onComplete() }
            )
              .fork
          } yield consumer
        }
      }
    }
//    }
  }
}
