package zio.streams.push.internal.operators

import zio.stm.TSemaphore
import zio.streams.push.PushStream.Operator
import zio.streams.push.internal.{Ack, Acks, Observer}
import zio.{Fiber, Queue, ZIO}

class MapParallelZioOperator[InA, OutR, OutE, OutB](parallelism: Int, f: InA => ZIO[OutR, OutE, OutB])
    extends Operator[InA, OutR, OutE, OutB] {
  override def apply[OutR1 <: OutR, OutE1 >: OutE](observer: Observer[OutR1, OutE1, OutB])
      : ZIO[OutR1, OutE1, Observer[OutR1, OutE1, InA]] = {
    TSemaphore.makeCommit(parallelism).flatMap { permits =>
      Queue.bounded[Fiber[OutE1, OutB]](parallelism * 2).flatMap { buffer =>
        var stop = false
        val consumer = new Observer[OutR1, OutE1, InA] {
          override def onNext(elem: InA): ZIO[OutR1, Nothing, Ack] = {
            if (stop) Acks.Stop
            else {
              for {
                _ <- permits.acquire.commit
                fiber <- f(elem).fork
                _ <- buffer.offer(fiber)
              } yield Ack.Continue
            }
          }

          override def onError(e: OutE1): ZIO[OutR1, OutE1, Unit] = {
            drainAndThen(observer.onError(e))
          }

          override def onComplete(): ZIO[OutR1, OutE1, Unit] = {
            drainAndThen(observer.onComplete())
          }

          private def drainAndThen(zio: ZIO[OutR1, OutE1, Unit]) = {
            permits.acquireN(parallelism).commit *> zio
          }
        }

        def take(): ZIO[OutR1, OutE1, Nothing] = {
          val runOnce = for {
            fiber <- buffer.take
            fiberResult <- fiber.join
            _ <- permits.release.commit
            ack <- observer.onNext(fiberResult)
            _ = if (ack == Ack.Stop) stop = true else ()
          } yield ()

          runOnce *> take()
        }

        for {
          _ <- take().fork
        } yield consumer
      }
    }
  }
}
