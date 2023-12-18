package zio.streams.push

import zio.stm.TSemaphore
import zio.streams.push.Ack.{Continue, Stop}
import zio.streams.push.PushStream.Operator
import zio.{Chunk, Fiber, Promise, Queue, Ref, UIO, Unsafe, ZIO}

trait Observer[R, E, -T] {
  def onNext(elem: T): ZIO[R, E, Ack]

  def onError(e: E): ZIO[R, E, Unit]

  def onComplete(): ZIO[R, E, Unit]
}

sealed abstract class Ack {}

object Acks {
  val Stop: UIO[Ack] = ZIO.succeed(Ack.Stop)
  val Continue: UIO[Ack] = ZIO.succeed(Ack.Continue)
}

object Ack {
  case object Stop extends Ack

  case object Continue extends Ack
}

trait PushStream[-R, +E, +A] { self =>
  def subscribe[OutR2 <: R, OutE2 >: E](observer: Observer[OutR2, OutE2, A]): ZIO[OutR2, OutE2, Unit]

  def map[B](f: A => B): PushStream[R, E, B] = new LiftByOperatorPushStream(this, new MapOperator[R, E, A, B](f))

  def mapZIO[R1 <: R, E1 >: E, A1](f: A => ZIO[R1, E1, A1]): PushStream[R1, E1, A1] =
    new LiftByOperatorPushStream(self, new MapZioOperator[A, R1, E1, A1](f))

  def mapZIOPar[R1 <: R, E1 >: E, A1](parallelism: Int)(f: A => ZIO[R1, E1, A1]): PushStream[R1, E1, A1] =
    new LiftByOperatorPushStream(this, new MapParallelZioOperator[A, R1, E1, A1](parallelism, f))

  def take(elements: Int): PushStream[R, E, A] = new LiftByOperatorPushStream(this, new TakeOperator[R, E, A](elements))

  def runCollect: ZIO[R, E, Chunk[A]] = runFold(Chunk.empty[A])((chunk, t) => chunk.appended(t))

  def mapConcat[A2](f: A => Iterable[A2]): PushStream[R, E, A2] = new LiftByOperatorPushStream(this, new MapConcatOperator[R, E, A, A2](f))

  def ++[R1 <: R, E1 >: E, A1 >: A](that: => PushStream[R1, E1, A1]): PushStream[R1, E1, A1] =
    self.concat(that)

  def orElse[R1 <: R, E1 >: E, A1 >: A](
      that: => PushStream[R1, E1, A1]): PushStream[R1, E1, A1] = {
    new PushStream[R1, E1, A1] {
      override def subscribe[OutR2 <: R1, OutE2 >: E1](observer: Observer[OutR2, OutE2, A1]): ZIO[OutR2, OutE2, Unit] = {
        self.subscribe(new DefaultObserver[OutR2, OutE2, A1](observer) {
          override def onNext(elem: A1): ZIO[OutR2, OutE2, Ack] = observer.onNext(elem)

          override def onError(e: OutE2): ZIO[OutR2, OutE2, Unit] = {
            ZIO.succeed(println("received error")) *> that.subscribe(observer)
          }
        })
      }
    }
  }

  def concat[R1 <: R, E1 >: E, A1 >: A](that: => PushStream[R1, E1, A1]): PushStream[R1, E1, A1] = {
    new PushStream[R1, E1, A1] {
      override def subscribe[OutR2 <: R1, OutE2 >: E1](observer: Observer[OutR2, OutE2, A1]): ZIO[OutR2, OutE2, Unit] = {
        self.subscribe(new DefaultObserver[OutR2, OutE2, A1](observer) {
          override def onNext(elem: A1): ZIO[OutR2, OutE2, Ack] = observer.onNext(elem)

          override def onComplete(): ZIO[OutR2, OutE2, Unit] = that.subscribe(observer)
        })
      }
    }
  }

  def runFold[B](z: B)(f: (B, A) => B): ZIO[R, E, B] = {
    Promise.make[Nothing, B].flatMap { completion =>
      var zState = z
      val stream: ZIO[R, E, Unit] = this.subscribe(new Observer[R, E, A] {
        override def onNext(elem: A): ZIO[R, E, Ack] = {
          ZIO.succeed {
            zState = f(zState, elem)
          }.as(Continue)

//          zState = f(zState, elem)
//          Acks.Continue
        }

        override def onError(e: E): ZIO[R, E, Unit] = ZIO.fail(e)

        override def onComplete(): UIO[Unit] = {
          completion.succeed(zState).unit
        }
      })

      val x: ZIO[R, E, B] = for {
        _ <- stream
        result <- completion.await
      } yield result

      x
    }
  }
}

object PushStream {

  trait Operator[InA, OutR, OutE, +OutB] {
    def apply[OutR1 <: OutR, OutE1 >: OutE](observer: Observer[OutR1, OutE1, OutB]): ZIO[OutR1, OutE1, Observer[OutR1, OutE1, InA]]
  }

  def apply[T](elems: T*): PushStream[Any, Nothing, T] = {
    fromIterable(elems)
  }

  def fromIterable[T](elems: Iterable[T]): PushStream[Any, Nothing, T] = {
    new SourcePushStream[T] {
      override def startSource[OutR, OutE](observer: Observer[OutR, OutE, T]): ZIO[OutR, OutE, Unit] = {
        val iterator = elems.iterator

        def emit(): ZIO[OutR, OutE, Unit] = {
          val hasNext = iterator.hasNext

          if (hasNext) Observers.emitOne(observer, iterator.next(), emit())
          else ZIO.unit
        }

        emit()
      }
    }
  }

  def range(start: Int, end: Int): PushStream[Any, Nothing, Int] = {
    new SourcePushStream[Int] {
      override def startSource[OutR2 <: Any, OutE2 >: Nothing](observer: Observer[OutR2, OutE2, Int]): ZIO[OutR2, OutE2, Unit] = {
        loop(start, observer)
      }

      private def loop[R, E](next: Int, observer: Observer[R, E, Int]): ZIO[R, E, Unit] = {
        if (next >= end) ZIO.unit
        else Observers.emitOne(observer, next, loop(next + 1, observer))
      }
    }
  }
}

trait SourcePushStream[A] extends PushStream[Any, Nothing, A] {
  override def subscribe[OutR2 <: Any, OutE2 >: Nothing](observer: Observer[OutR2, OutE2, A]): ZIO[OutR2, OutE2, Unit] = {
    startSource(observer).either.flatMap {
      case Left(error) => observer.onError(error)
      case Right(()) => observer.onComplete()
    }
  }

  def startSource[OutR2 <: Any, OutE2 >: Nothing](observer: Observer[OutR2, OutE2, A]): ZIO[OutR2, OutE2, Unit]
}

class LiftByOperatorPushStream[InR, InE, InA, OutR <: InR, OutE >: InE, OutB](
    upstream: PushStream[InR, InE, InA],
    operator: Operator[InA, OutR, OutE, OutB]) extends PushStream[OutR, OutE, OutB] {
  override def subscribe[OutR2 <: OutR, OutE2 >: OutE](observer: Observer[OutR2, OutE2, OutB]): ZIO[OutR2, OutE2, Unit] = {
    val transformedObserver: ZIO[OutR2, OutE2, Observer[OutR2, OutE2, InA]] = operator(observer)

    val subscribedObserver: ZIO[OutR2, OutE2, Unit] = transformedObserver
      .flatMap(subscriberB => upstream.subscribe(subscriberB))
    subscribedObserver
  }
}

class MapOperator[R, E, A, B](f: A => B) extends Operator[A, R, E, B] {
  override def apply[OutR1 <: R, OutE1 >: E](out: Observer[OutR1, OutE1, B]): UIO[Observer[OutR1, OutE1, A]] =
    ZIO.succeed(new DefaultObserver[OutR1, OutE1, A](out) {
      override def onNext(elem: A): ZIO[OutR1, OutE1, Ack] = {
        out.onNext(f(elem))
      }
    })
}

class MapConcatOperator[R, E, A, B](f: A => Iterable[B]) extends Operator[A, R, E, B] {
  override def apply[OutR1 <: R, OutE1 >: E](out: Observer[OutR1, OutE1, B]): UIO[Observer[OutR1, OutE1, A]] =
    ZIO.succeed(new DefaultObserver[OutR1, OutE1, A](out) {
      override def onNext(elem: A): ZIO[OutR1, OutE1, Ack] = {
        Observers.emitAll(out, f(elem))
      }
    })

}

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
            onComplete() // TODO pass through errors
          }

          override def onComplete(): ZIO[OutR1, OutE1, Unit] = {
            permits.acquireN(parallelism).commit *> observer.onComplete()
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

class MapZioOperator[InA, OutR, OutE, OutB](f: InA => ZIO[OutR, OutE, OutB]) extends Operator[InA, OutR, OutE, OutB] {
  def apply[OutR1 <: OutR, OutE1 >: OutE](out: Observer[OutR1, OutE1, OutB]): UIO[Observer[OutR1, OutE1, InA]] =
    ZIO.succeed(new DefaultObserver[OutR1, OutE1, InA](out) {

      override def onNext(elem: InA): ZIO[OutR1, OutE1, Ack] = {
        val result: ZIO[OutR1, OutE1, Ack] = f(elem).flatMap(b => out.onNext(b))
        result
      }
    })
}

class TakeOperator[R, E, A](n: Long) extends Operator[A, R, E, A] {

  override def apply[OutR1 <: R, OutE1 >: E](out: Observer[OutR1, OutE1, A]): ZIO[OutR1, OutE1, Observer[OutR1, OutE1, A]] =
    Ref.make(0).map { ref =>
      new DefaultObserver[OutR1, OutE1, A](out) {
        override def onNext(elem: A): ZIO[OutR1, OutE1, Ack] = {
          ref.updateAndGet(i => i + 1).flatMap(emitted => if (emitted > n) out.onComplete().as(Stop) else out.onNext(elem))
        }
      }
    }
}

trait DefaultObserver[R, E, A](out: Observer[R, E, _]) extends Observer[R, E, A] {
  def onError(e: E): ZIO[R, E, Unit] = out.onError(e)

  override def onComplete(): ZIO[R, E, Unit] = out.onComplete()
}
