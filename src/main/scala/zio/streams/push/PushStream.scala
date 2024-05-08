package zio.streams.push

import zio.Dequeue
import zio.stream.ZStream
import zio.stream.ZStream.{scoped, unfoldChunkZIO}
import zio.streams.push.internal.*
import zio.streams.push.internal.Ack.{Continue, Stop}
import zio.streams.push.internal.operators.*
import zio.{Chunk, Exit, Promise, Queue, Schedule, Scope, Trace, UIO, URIO, Unsafe, ZIO}

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

  /** Threads the stream through the transformation function `f`.
    */
  def viaFunction[R2, E2, B](f: PushStream[R, E, A] => PushStream[R2, E2, B])(implicit trace: Trace): PushStream[R2, E2, B] =
    f(self)

  def take(elements: Int): PushStream[R, E, A] = {
    if (elements <= 0) PushStream.empty
    else new LiftByOperatorPushStream(this, new TakeOperator[R, E, A](elements))
  }

  /** Adds an effect to consumption of every element of the stream.
    */
  def tap[R1 <: R, E1 >: E](f: A => ZIO[R1, E1, Any])(implicit trace: Trace): PushStream[R1, E1, A] =
    mapZIO(a => f(a).as(a))

  def runCollect: ZIO[R, E, Chunk[A]] = runFold(Chunk.empty[A])((chunk, t) => chunk.appended(t))

  /** Runs the stream only for its effects. The emitted elements are discarded.
    */
  def runDrain(implicit trace: Trace): ZIO[R, E, Unit] = runCollect.unit

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

  def buffer(capacity: => Int)(implicit trace: Trace): PushStream[R, E, A] = {
    val queueDef: ZIO[Any with Scope, Nothing, Queue[A]] =
      ZIO.acquireRelease(Queue.bounded[A](capacity))(_.shutdown)

    val x: ZIO[Any with Scope, Nothing, LiftByOperatorPushStream[R, E, A, R, E, A]] =
      queueDef.map(queue => new LiftByOperatorPushStream(this, new BufferOperator[A, R, E](queue)))

    PushStream.unwrapScoped(x)
  }

  def bufferSliding(capacity: => Int)(implicit trace: Trace): PushStream[R, E, A] = {
    val queueDef: ZIO[Any with Scope, Nothing, Queue[A]] =
      ZIO.acquireRelease(Queue.sliding[A](capacity))(_.shutdown)

    val x: ZIO[Any with Scope, Nothing, LiftByOperatorPushStream[R, E, A, R, E, A]] =
      queueDef.map(queue => new LiftByOperatorPushStream(this, new BufferOperator[A, R, E](queue)))

    PushStream.unwrapScoped(x)
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

  def flatMap[R1 <: R, E1 >: E, B](f: A => PushStream[R1, E1, B])(implicit trace: Trace): PushStream[R1, E1, B] = {
    new PushStream[R1, E1, B] {
      override def subscribe[OutR2 <: R1, OutE2 >: E1](observer: Observer[OutR2, OutE2, B]): URIO[OutR2, Unit] = {
        // ZIO.suspendSucceed is used to make flatMap stack safe
        ZIO.suspendSucceed(
          self.subscribe(new DefaultObserver[OutR2, OutE2, A](observer) {
            override def onNext(elem: A): URIO[OutR2, Ack] = {
              var lastAck: Ack = Ack.Continue
              val newStream: PushStream[R1, E1, B] = f(elem)
              newStream.subscribe(new Observer[OutR2, OutE2, B] {
                override def onNext(innerElem: B): URIO[OutR2, Ack] = {
                  // TODO -- if downstream requests a stop here, signal that back up.
                  observer.onNext(innerElem).tap { ack =>
                    ZIO.succeed {
                      lastAck = ack
                    }
                  }
                }

                override def onError(e: OutE2): URIO[OutR2, Unit] = {
                  lastAck = Ack.Stop
                  observer.onError(e)
                }

                override def onComplete(): URIO[OutR2, Unit] = ZIO.unit // ignore completion of intermediate
              }).flatMap { _ =>
                // TODO like the take operator, this would benefit from the subscribe method returning the last ack
                ZIO.succeed(lastAck)
              }
            }
          })
        )
      }
    }
  }

  /** Flattens this stream-of-streams into a stream made of the concatenation in strict order of all the streams.
    */
  def flatten[R1 <: R, E1 >: E, A1](implicit ev: A <:< PushStream[R1, E1, A1], trace: Trace): PushStream[R1, E1, A1] =
    flatMap(ev(_))

  def concat[R1 <: R, E1 >: E, A1 >: A](that: => PushStream[R1, E1, A1]): PushStream[R1, E1, A1] = {
    new PushStream[R1, E1, A1] {
      override def subscribe[OutR2 <: R1, OutE2 >: E1](observer: Observer[OutR2, OutE2, A1]): URIO[OutR2, Unit] = {
        self.subscribe(new DefaultObserver[OutR2, OutE2, A1](observer) {
          var lastAck: Ack = Ack.Continue
          override def onNext(elem: A1): URIO[OutR2, Ack] = observer.onNext(elem).tap(ack => ZIO.succeed { lastAck = ack })

          override def onComplete(): URIO[OutR2, Unit] = {
            // TODO A potential alternative is to have the onNext method return the
            if (lastAck == Ack.Continue)
              that.subscribe(observer)
            else ZIO.unit // if downstream said nothing else please, then don't start subscribing to something else
          }
        })
      }
    }
  }

  /** Composes this stream with the specified stream to create a cartesian product of elements, but keeps only elements from the other
    * stream. The `that` stream would be run multiple times, for every element in the `this` stream.
    *
    * See also [[ZStream#zip]] and [[ZStream#<&>]] for the more common point-wise variant.
    */
  def crossRight[R1 <: R, E1 >: E, B](that: => PushStream[R1, E1, B])(implicit trace: Trace): PushStream[R1, E1, B] =
    self.flatMap(_ => that)

  /** Symbolic alias for [[ZStream#crossRight]].
    */
  def *>[R1 <: R, E1 >: E, A2](that: => PushStream[R1, E1, A2])(implicit trace: Trace): PushStream[R1, E1, A2] =
    self.crossRight(that)

  /** Executes the provided finalizer after this stream's finalizers run.
    */
  def ensuring[R1 <: R](fin: => ZIO[R1, Nothing, Any])(implicit trace: Trace): PushStream[R1, E, A] =
    PushStream.acquireReleaseWith(ZIO.succeed(self))(_ => fin).flatten

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

}

object PushStream {

  trait Operator[InA, OutR, OutE, +OutB] {
    def apply[OutR1 <: OutR, OutE1 >: OutE](observer: Observer[OutR1, OutE1, OutB]): URIO[OutR1, Observer[OutR1, OutE1, InA]]
  }

  def apply[T](elems: T*): PushStream[Any, Nothing, T] = {
    fromIterable(elems)
  }

  def succeed[T](elem: T): PushStream[Any, Nothing, T] = {
    apply(elem)
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

  /** The stream that never produces any value or fails with any error.
    */
  def never(implicit trace: Trace): PushStream[Any, Nothing, Nothing] =
    PushStream.fromZIO(ZIO.never)

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

  /** Creates a stream from a single value that will get cleaned up after the stream is consumed
    */
  def acquireReleaseWith[R, E, A](acquire: => ZIO[R, E, A])(release: A => URIO[R, Any])(implicit trace: Trace): PushStream[R, E, A] = {
    val scopedStream: ZIO[R with Scope, E, PushStream[Any, Nothing, A]] = ZIO.acquireRelease(acquire)(release).map(a => PushStream(a))
    unwrapScoped(scopedStream)
  }

}
