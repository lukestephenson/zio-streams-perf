package zio.streams.push.internal.operators

import zio.stm.TSemaphore
import zio.streams.push.PushStream.Operator
import zio.streams.push.internal.{Ack, Acks, Observer}
import zio.{Fiber, Promise, Queue, ZIO}

class MapParallelZioOperator[InA, OutR, OutE, OutB](parallelism: Int, f: InA => ZIO[OutR, OutE, OutB], name: String)
    extends Operator[InA, OutR, OutE, OutB] {

  private enum Complete {
    case Done
  }

  override def apply[OutR1 <: OutR, OutE1 >: OutE](observer: Observer[OutR1, OutE1, OutB])
      : ZIO[OutR1, OutE1, Observer[OutR1, OutE1, InA]] = {
    TSemaphore.makeCommit(parallelism).flatMap { permits =>
      Queue.bounded[Fiber[OutE1, OutB]](parallelism * 2).flatMap { buffer =>
        Promise.make[OutE1, Complete].flatMap { failurePromise =>
          Promise.make[Nothing, Complete].flatMap { shutdownPromise =>
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
                    _ <- buffer.offer(fiber)
                  } yield if (stop) Ack.Stop else Ack.Continue
                }
              }

              override def onError(e: OutE1): ZIO[OutR1, OutE1, Unit] = {
                // for errors, we want to interrupt any outstanding work immediately
                zio.Console.printLine(s"$name - onError").ignore *> failurePromise.fail(e).unit *> zio.Console.printLine(
                  s"$name - onError done"
                ).ignore *> shutdownPromise.await.unit
              }

              override def onComplete(): ZIO[OutR1, OutE1, Unit] = {
                // for completion, we want to allow all outstanding fibers to complete, and then signal completion (but only if
                // obsever.onError hasn't been called already)
                zio.Console.printLine(s"$name - onComplete").ignore *>
                  permits.acquireN(parallelism).commit *>
                  zio.Console.printLine(s"$name - onComplete permitsAcquired").ignore *>
                  failurePromise.succeed(Complete.Done).unit *>
                  shutdownPromise.await.unit *>
                  zio.Console.printLine(s"$name - onComplete done").ignore
              }
            }

            def cancelRunningJobs() = {
              for {
                fibers <- buffer.takeAll
                _ <- zio.Console.printLine(s"$name - size ${fibers.size} cancelling fibers").either
                _ <- ZIO.foreachDiscard(fibers)(f =>
                  zio.Console.printLine(s"$name - size ${fibers.size} - about to interrupt").either *> f.interrupt *> zio.Console.printLine(
                    s"$name - size ${fibers.size} - done interrupt"
                  ).either
                )
                _ <- zio.Console.printLine(s"$name - size ${fibers.size} done cancelling fibers").either
                //              _ <- buffer.shutdown
                _ <- permits.releaseN(parallelism).commit
                _ <- zio.Console.printLine(s"$name - size ${fibers.size} done releasing permits").either
              } yield ()
            }

            def take(): ZIO[OutR1, OutE1, Nothing] = {
              val runOnce = for {
                fiber <- buffer.take
                //              _ <- ZIO.raceFirst(fiber.join, List(failurePromise.await)).foldCauseZIO(
                //                failure = { cause =>
                //                  failurePromise.failCause(cause) *> fiber.interrupt.unit
                //                },
                //                success = { fiberResult =>
                //                  if (fiberResult == Complete)
                //                    ZIO.dieMessage("should never reach this point")
                //                  else
                //                    observer.onNext(fiberResult.asInstanceOf[OutB]).map(ack => if (ack == Ack.Stop) stop = true else ()).unit
                //                }
                //              )
                fiberResult <- fiber.join.onInterrupt(fiber.interrupt).onError(cause =>
                  failurePromise.failCause(cause) *> zio.Console.printLine(s"$name - fiber.join failed with $cause").ignore
                )
                _ <- observer.onNext(fiberResult).map(ack => if (ack == Ack.Stop) stop = true else ()).unit
                _ <- permits.release.commit
              } yield ()

              runOnce *> take()
            }

            for {
              takeFiber <- take().onExit(exit => zio.Console.printLine(s"$name - take has exited $exit").ignore).forkDaemon
              // Only call onError once on downstream. TODO, handle other failure types
              _ <- failurePromise.await
                //              .onError(cause => zio.Console.printLine(s"$name - failed with $cause").ignore)
                .foldCauseZIO(
                  failure = { cause =>
                    zio.Console.printLine(s"$name - failed with $cause") *>
                      cancelRunningJobs() *>
                      ZIO.succeed {
                        stop = true
                      } *> ZIO.foreachDiscard(
                        cause.failureOption
                      )(e => observer.onError(e).either)
                  },
                  success = { _ => observer.onComplete() }
                )
                .ensuring(zio.Console.printLine(s"$name - interrupt takeFiber").ignore *> takeFiber.interrupt)
                .ensuring(shutdownPromise.succeed(Complete.Done))
                .fork
            } yield consumer
          }
        }
      }
    }
  }
}

// [info] Benchmark                             Mode  Cnt  Score   Error   Units
//[info] Benchmarks.pStreamMapZioPar          thrpt    9  0.068 ± 0.005  ops/us
//[info] Benchmarks.pStreamMapZioParChunk100  thrpt    9  5.789 ± 0.171  ops/us
//[info] Benchmarks.zStreamMapZioParChunk1    thrpt    9  0.053 ± 0.002  ops/us
//[info] Benchmarks.zStreamMapZioParChunk100  thrpt    9  0.083 ± 0.007  ops/us
