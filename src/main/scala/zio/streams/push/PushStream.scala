package zio.streams.push

import zio.streams.push.internal.*
import zio.streams.push.internal.Ack.{Continue, Stop}
import zio.streams.push.internal.operators.*
import zio.{Chunk, Promise, Trace, UIO, URIO, Unsafe, ZIO, Scope, Exit, Schedule}

trait PushStream[-R, +E, +A] { self =>
  def subscribe[OutR2 <: R, OutE2 >: E](observer: Observer[OutR2, OutE2, A]): URIO[OutR2, Unit]

  def map[B](f: A => B): PushStream[R, E, B] = new LiftByOperatorPushStream(this, new MapOperator[R, E, A, B](f))

  def mapZIO[R1 <: R, E1 >: E, A1](f: A => ZIO[R1, E1, A1]): PushStream[R1, E1, A1] =
    new LiftByOperatorPushStream(self, new MapZioOperator[A, R1, E1, A1](f))

  def mapZIOPar[R1 <: R, E1 >: E, A1](parallelism: Int, name: String = "default")(f: A => ZIO[R1, E1, A1]): PushStream[R1, E1, A1] =
    new LiftByOperatorPushStream(this, new MapParallelZioOperator[A, R1, E1, A1](parallelism, f, name))

  def scan[S](s: => S)(f: (S, A) => S)(implicit trace: Trace): PushStream[R, E, S] =
    scanZIO(s)((s, a) => ZIO.succeed(f(s, a)))

  def scanZIO[R1 <: R, E1 >: E, S](s: => S)(f: (S, A) => ZIO[R1, E1, S])(implicit trace: Trace): PushStream[R1, E1, S] = {
    new ScanZioPushStream(self, s, f)
  }

  def take(elements: Int): PushStream[R, E, A] = new LiftByOperatorPushStream(this, new TakeOperator[R, E, A](elements))

  def runCollect: ZIO[R, E, Chunk[A]] = runFold(Chunk.empty[A])((chunk, t) => chunk.appended(t))

  def mapConcat[A2](f: A => Iterable[A2]): PushStream[R, E, A2] = new LiftByOperatorPushStream(this, new MapConcatOperator[R, E, A, A2](f))

  def collect[B](f: PartialFunction[A, B])(implicit trace: Trace): PushStream[R, E, B] =
    mapConcat(a => f.lift(a))

  def ++[R1 <: R, E1 >: E, A1 >: A](that: => PushStream[R1, E1, A1]): PushStream[R1, E1, A1] =
    self.concat(that)

  /** Schedules the output of the stream using the provided `schedule` and emits its output at the end (if `schedule` is finite).
    */
  def scheduleEither[R1 <: R, E1 >: E, B](
      schedule: => Schedule[R1, A, B])(implicit trace: Trace): PushStream[R1, E1, Either[B, A]] =
    scheduleWith(schedule)(Right.apply, Left.apply)

  /** Schedules the output of the stream using the provided `schedule`.
    */
  def schedule[R1 <: R](schedule: => Schedule[R1, A, Any])(implicit trace: Trace): PushStream[R1, E, A] =
    scheduleEither(schedule).collect { case Right(a) => a }

  /** Schedules the output of the stream using the provided `schedule` and emits its output at the end (if `schedule` is finite). Uses the
    * provided function to align the stream and schedule outputs on the same type.
    */
  def scheduleWith[R1 <: R, E1 >: E, B, C](
      schedule: => Schedule[R1, A, B])(f: A => C, g: B => C)(implicit trace: Trace): PushStream[R1, E1, C] = {
    new LiftByOperatorPushStream(this, new ScheduleOperator[A, R1, E1, B, C](schedule, f, g))
  }

  def orElse[R1 <: R, E1 >: E, A1 >: A](
      that: => PushStream[R1, E1, A1]): PushStream[R1, E1, A1] = {
    new PushStream[R1, E1, A1] {
      override def subscribe[OutR2 <: R1, OutE2 >: E1](observer: Observer[OutR2, OutE2, A1]): URIO[OutR2, Unit] = {
        self.subscribe(new DefaultObserver[OutR2, OutE2, A1](observer) {
          override def onNext(elem: A1): URIO[OutR2, Ack] = observer.onNext(elem)

          override def onError(e: OutE2): URIO[OutR2, Unit] = {
            that.subscribe(observer)
          }
        })
      }
    }
  }

  def concat[R1 <: R, E1 >: E, A1 >: A](that: => PushStream[R1, E1, A1]): PushStream[R1, E1, A1] = {
    new PushStream[R1, E1, A1] {
      override def subscribe[OutR2 <: R1, OutE2 >: E1](observer: Observer[OutR2, OutE2, A1]): URIO[OutR2, Unit] = {
        self.subscribe(new DefaultObserver[OutR2, OutE2, A1](observer) {
          override def onNext(elem: A1): URIO[OutR2, Ack] = observer.onNext(elem)

          override def onComplete(): URIO[OutR2, Unit] = that.subscribe(observer)
        })
      }
    }
  }

  def runFold[B](z: B)(f: (B, A) => B): ZIO[R, E, B] = {
    Promise.make[E, B].flatMap { completion =>
      var zState = z
      val stream: URIO[R, Unit] = this.subscribe(new Observer[R, E, A] {
        override def onNext(elem: A): UIO[Ack] = {
          ZIO.succeed {
            zState = f(zState, elem)
            Continue
          }
        }

        override def onError(e: E): UIO[Unit] = completion.fail(e).unit

        override def onComplete(): UIO[Unit] = {
          // TODO consider using a ref / compare performance
          ZIO.suspendSucceed(completion.succeed(zState).unit)
        }
      })

      for {
        _ <- stream
        result <- completion.await
      } yield result
    }
  }

  def runDrain(implicit trace: Trace): ZIO[R, E, Unit] = {
    // TODO find a common abstraction here
    runFold(())((_, _) => ()).unit
  }
}

object PushStream {

  trait Operator[InA, OutR, OutE, +OutB] {
    def apply[OutR1 <: OutR, OutE1 >: OutE](observer: Observer[OutR1, OutE1, OutB]): URIO[OutR1, Observer[OutR1, OutE1, InA]]
  }

  def apply[T](elems: T*): PushStream[Any, Nothing, T] = {
    fromIterable(elems)
  }

  def empty[T]: PushStream[Any, Nothing, T] = fromIterable(Iterable.empty)

  def fail[E](error: => E)(implicit trace: Trace): PushStream[Any, E, Nothing] =
    fromZIO(ZIO.fail(error))

  def fromZIO[R, E, A](fa: => ZIO[R, E, A])(implicit trace: Trace): PushStream[R, E, A] =
    new SourcePushStream[R, E, A] {
      override protected def startSource[OutR2 <: R, OutE2 >: E](observer: Observer[OutR2, OutE2, A]): URIO[OutR2, Unit] = {
        fa.foldZIO(observer.onError, observer.onNext).unit
      }
    }

  def unwrapScoped[R, E, A](f: => ZIO[R with Scope, E, PushStream[R, E, A]])(implicit trace: Trace): PushStream[R, E, A] = {
    new PushStream[R, E, A] {
      override def subscribe[OutR2 <: R, OutE2 >: E](observer: Observer[OutR2, OutE2, A]): URIO[OutR2, Unit] = {
        ZIO.uninterruptibleMask { restore =>
          Scope.make.flatMap { scope =>
//            val newObserver = new Observer[OutR2, OutE2, A] {
//              override def onNext(elem: A): URIO[OutR2, Ack] = observer.onNext(elem)
//
//              override def onError(e: OutE2): URIO[OutR2, Unit] = observer.onError(e)
//
//              override def onComplete(): URIO[OutR2, Unit] = observer.onComplete()
//            }
            // TODO remove the orDie
            restore(scope.extend(f)).foldZIO(
              failure =>
              zio.Console.printLine("failure").ignore *>
                observer.onError(failure) *> scope.close(Exit.fail(failure)),
              { downstream =>
                val downSub: URIO[OutR2, Unit] = restore(downstream.subscribe(observer))
                val y: URIO[OutR2, Unit] =
                  downSub.onExit(exit => scope.close(exit)).unit
                zio.Console.printLine("got downstream").ignore *> y
              }
            ).onExit(exit => scope.close(exit))
          }
        }
      }
    }
  }

//  def unwrapScoped2[R, E, A](f: => ZIO[R with Scope, E, PushStream[R, E, A]])(implicit trace: Trace): PushStream[R, E, A] = {
//    new PushStream[R, E, A] {
//      override def subscribe[OutR2 <: R, OutE2 >: E](observer: Observer[OutR2, OutE2, Int]): URIO[OutR2, Unit] = {
//        zio.Console.printLine(s"inside subscribe").ignore *>
//          Scope.make.flatMap { scope =>
//            scope.extend(f).flatMap { downstream =>
//              zio.Console.printLine(s"got the downstream").ignore *>
//                downstream.subscribe(observer).onExit(exit => zio.Console.printLine(s"downstream emitted elements, we can close the scope").ignore *> scope.close(exit)).unit
//            }
//          }
//      }
//    }
//  }
//  def foo(): UIO[Unit] = {
//    import zio.durationInt
//
//    def streamGen(i: Int): PushStream[Any, Nothing, Int] =
//      PushStream.range(1,5).mapZIO(i => zio.Console.printLine(s"hello $i").ignore.delay(1.second).as(i))
//
//    val scoped: ZIO[Any with Scope, Nothing, PushStream[Any, Nothing, Int]] = ZIO.acquireRelease(zio.Console.printLine("acquire").ignore.as(5))(i =>zio.Console.printLine(s"closing $i").ignore).map(resource => streamGen(resource))
//
//    val stream = new PushStream[Any, Nothing, Int] {
//
//      override def subscribe[OutR2 <: Any, OutE2 >: Nothing](observer: Observer[OutR2, OutE2, Int]): URIO[OutR2, Unit] = {
//        val x: ZIO[OutR2, Nothing, Unit] = zio.Console.printLine(s"inside subscribe").ignore *>
//        Scope.make.flatMap { scope =>
//          scope.extend(scoped).flatMap { downstream =>
//            zio.Console.printLine(s"got the downstream").ignore *>
//            downstream.subscribe(observer).onExit(exit => zio.Console.printLine(s"downstream emitted elements, we can close the scope").ignore *> scope.close(exit)).unit
//          }
//        }
//        x
//      }
//    }
//
//    val job = stream
//
//    job.runCollect.unit
//  }
  /** Creates a stream from an effect producing values of type `A` until it fails with None.
    */
  def repeatZIOOption[R, E, A](fa: => ZIO[R, Option[E], A])(implicit trace: Trace): PushStream[R, E, A] = {
    new SourcePushStream[R, E, A] {
      override protected def startSource[OutR2 <: R, OutE2 >: E](observer: Observer[OutR2, OutE2, A]): URIO[OutR2, Unit] = {
        def pullOnce(): URIO[OutR2, Unit] = {
          fa.foldZIO(
            failure = {
              case None => observer.onComplete()
              case Some(error) => observer.onError(error)
            },
            success = item => Observers.emitOne(observer, item, pullOnce())
          )
        }

        pullOnce()
      }
    }
  }

  def fromIterable[T](elems: Iterable[T]): PushStream[Any, Nothing, T] = {
    new SourcePushStream[Any, Nothing, T] {
      override def startSource[OutR, OutE](observer: Observer[OutR, OutE, T]): ZIO[OutR, OutE, Unit] = {
        Observers.emitAll(observer, elems).unit
      }
    }
  }

  def range(start: Int, end: Int): PushStream[Any, Nothing, Int] = {
    new SourcePushStream[Any, Nothing, Int] {
      override def startSource[OutR2 <: Any, OutE2 >: Nothing](observer: Observer[OutR2, OutE2, Int]): ZIO[OutR2, OutE2, Unit] = {
        loop(start, observer)
      }

      private def loop[R, E](next: Int, observer: Observer[R, E, Int]): URIO[R, Unit] = {
        when(next < end)(Observers.emitOne(observer, next, loop(next + 1, observer)))
      }
    }
  }
}
