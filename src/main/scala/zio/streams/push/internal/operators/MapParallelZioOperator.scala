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
        // TODO The Complete side of this promise is a bit messy when racing for failures. Is there a better alternative.
        Promise.make[OutE1, Complete].flatMap { failurePromise =>
          var stop = false

          val consumer = new Observer[OutR1, OutE1, InA] {
            override def onNext(elem: InA): ZIO[OutR1, Nothing, Ack] = {
              if (stop) Acks.Stop
              else {
                for {
                  _ <- permits.acquire.commit
                  fiber <- f(elem).onError { cause =>
                    failurePromise.failCause(cause) *> ZIO.succeed {
                      stop = true
                    }
                  }.fork
                  _ <-  buffer.offer(fiber)
                } yield Ack.Continue
              }
            }

            override def onError(e: OutE1): ZIO[OutR1, OutE1, Unit] = {
              // for errors, we want to interrupt any outstanding work immediately
              failurePromise.fail(e).unit
            }

            override def onComplete(): ZIO[OutR1, OutE1, Unit] = {
              // for completion, we want to allow all outstanding fibers to complete, and then signal completion (but only if
              // obsever.onError hasn't been called already)
                permits.acquireN(parallelism).commit *> failurePromise.succeed(Complete.Done).unit
            }
          }

          def take(): ZIO[OutR1, OutE1, Nothing] = {
            val runOnce = for {
              fiber <- buffer.take
              _ <- ZIO.raceFirst(fiber.join, List(failurePromise.await)).foldCauseZIO(
                failure = { cause =>
                  failurePromise.failCause(cause) *> fiber.interrupt.unit
                },
                success = { fiberResult =>
                  if (fiberResult == Complete)
                    ZIO.dieMessage("should never reach this point")
                  else
                    observer.onNext(fiberResult.asInstanceOf[OutB]).map(ack => if (ack == Ack.Stop) stop = true else ()).unit
                }
              )
              _ <- permits.release.commit
            } yield ()

            runOnce *> take()
          }

          for {
            takeFiber <- take().fork
            // Only call onError once on downstream. TODO, handle other failure types
            _ <- failurePromise.await.foldCauseZIO(
              failure = { cause =>
                ZIO.succeed {
                  stop = true
                } *> ZIO.foreachDiscard(
                  cause.failureOption
                )(e => observer.onError(e).either)
              },
              success = { _ => observer.onComplete() }
            )
              .fork
          } yield consumer
        }
      }
    }
  }
}
