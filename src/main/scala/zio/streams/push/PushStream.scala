package zio.streams.push

import zio.stream.ZStream
import zio.stream.ZStream.fromZIOOption
import zio.streams.push.internal.*
import zio.streams.push.internal.Ack.{Continue, Stop}
import zio.streams.push.internal.operators.*
import zio.{Chunk, Promise, Scope, Trace, UIO, Unsafe, ZIO, ZIOApp, ZIOAppArgs, ZIOAppDefault}
import zio.durationInt

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
    Promise.make[E, B].flatMap { completion =>
      var zState = z
      val stream: ZIO[R, E, Unit] = this.subscribe(new Observer[R, E, A] {
        override def onNext(elem: A): ZIO[R, E, Ack] = {
          ZIO.succeed {
            zState = f(zState, elem)
          }.as(Continue)

//          zState = f(zState, elem)
//          Acks.Continue
        }

        override def onError(e: E): ZIO[R, E, Unit] = zio.Console.printLine("fold picked up a failure").either *> completion.fail(e).unit

        override def onComplete(): UIO[Unit] = {
          // TODO consider using a ref / compare performance
          ZIO.suspendSucceed(completion.succeed(zState).unit)
        }
      }).onExit(exit => zio.Console.printLine(s"runFold stream $exit").ignore)

      val x: ZIO[R, E, B] = for {
        _ <- stream
        _ <- zio.Console.printLine("stream has terminated").ignore
        result <- completion.await
      } yield result

      x.onExit(exit => zio.Console.printLine(s"runFold $exit").ignore)
    }
  }

  def runDrain(implicit trace: Trace): ZIO[R, E, Unit] = {
    // TODO find a common abstraction here
    runFold(())((_, _) => ()).unit
  }
}

object PushStream {

  trait Operator[InA, OutR, OutE, +OutB] {
    def apply[OutR1 <: OutR, OutE1 >: OutE](observer: Observer[OutR1, OutE1, OutB]): ZIO[OutR1, OutE1, Observer[OutR1, OutE1, InA]]
  }

  def apply[T](elems: T*): PushStream[Any, Nothing, T] = {
    fromIterable(elems)
  }

  def fail[E](error: => E)(implicit trace: Trace): PushStream[Any, E, Nothing] =
    fromZIO(ZIO.fail(error))

  def fromZIO[R, E, A](fa: => ZIO[R, E, A])(implicit trace: Trace): PushStream[R, E, A] =
    new SourcePushStream2[R, E, A]:
      override protected def startSource[OutR2 <: R, OutE2 >: E](observer: Observer[OutR2, OutE2, A]): ZIO[OutR2, OutE2, Unit] = {
        fa.flatMap(observer.onNext).unit
      }

  def fromIterable[T](elems: Iterable[T]): PushStream[Any, Nothing, T] = {
    new SourcePushStream[T] {
      override def startSource[OutR, OutE](observer: Observer[OutR, OutE, T]): ZIO[OutR, OutE, Unit] = {
        Observers.emitAll(observer, elems).unit *> zio.Console.printLine("done emitting elements").ignore
      }
    }
  }

  def range(start: Int, end: Int): PushStream[Any, Nothing, Int] = {
    new SourcePushStream[Int] {
      override def startSource[OutR2 <: Any, OutE2 >: Nothing](observer: Observer[OutR2, OutE2, Int]): ZIO[OutR2, OutE2, Unit] = {
        loop(start, observer)
      }

      private def loop[R, E](next: Int, observer: Observer[R, E, Int]): ZIO[R, E, Unit] = {
        when(next < end)(Observers.emitOne(observer, next, loop(next + 1, observer)))
      }
    }
  }
}

object Foo extends ZIOAppDefault {
  override def run: ZIO[Foo.Environment with ZIOAppArgs with Scope, Any, Any] = {
    for {
      result <- PushStream.fromIterable(1 to 4).mapZIOPar(953)(a => ZIO.succeed(a).delay(1.millisecond)).runCollect
      isSorted = List.from(1 to 4) == result.toList
      _ <- zio.Console.printLine(s"$isSorted - $result")
      job = (zio.Console.printLine("never fiber") *> ZIO.never *> zio.Console.printLine("not expected")).onInterrupt(zio.Console.printLine(
        "1 - interrupted"
      ).either)
      fiber <- job.fork
      _ <-
        ZIO.sleep(1.second) *> zio.Console.printLine("interrupt fiber") *> fiber.interrupt *> zio.Console.printLine("done interrupt fiber")
    } yield ()
  }
}
