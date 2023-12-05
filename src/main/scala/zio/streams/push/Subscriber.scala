package zio.streams.push

import zio.profiling.sampling._
import zio.stm.TSemaphore
import zio.stream.ZStream
import zio.streams.push.Ack.{Continue, Stop}
import zio.streams.push.PushStream.Operator
import zio.{Chunk, Exit, Fiber, Promise, Queue, Ref, Scope, Semaphore, Task, Trace, UIO, Unsafe, ZIO, ZIOAppArgs, ZIOAppDefault, durationInt}

import java.util.concurrent.ConcurrentLinkedQueue
import scala.annotation.tailrec
import java.util.concurrent.{Semaphore => JSemaphore}

trait Observer[R, +E, -T] {
  def onNext(elem: T): ZIO[R, E, Ack]

  def onComplete(): UIO[Unit]
}

sealed abstract class Ack {

}

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

  def map[B](f: A => B): PushStream[R, E, B] = new LiftByOperatorPushStream(this, new MapOperator[R,E,A, B](f))

  def mapZIO[R1 <: R, E1 >: E, A1](f: A => ZIO[R1, E1, A1]): PushStream[R1, E1, A1] = new LiftByOperatorPushStream(self, new MapZioOperator[A, R1, E1, A1](f))

//  def mapZioPar[B](parallelism: Int)(f: A => UIO[B]): PushStream[R, E, B] = new LiftByOperatorPushStream(this, new MapParallelZioOperator(parallelism, f))
////  def mapZioParFast[B](parallelism: Int)(f: A => UIO[B]): PushStream[R, E, B] = new LiftByOperatorPushStream(this, new MapParallelZioOperatorFast(parallelism, f))
//
//  def take(elements: Int): PushStream[R, E, A] = new LiftByOperatorPushStream(this, new TakeOperator[A](elements))

  def runCollect: ZIO[R, E, Chunk[A]] = runFold(Chunk.empty[A])((chunk, t) => chunk.appended(t))

//  def ++[R1 <: R, E1 >: E, A1 >: A](that: => PushStream[R1, E1, A1]): PushStream[R1, E1, A1] =
//    self concat that
//
//  def concat[R1 <: R, E1 >: E, A1 >: A](that: => PushStream[R1, E1, A1]): PushStream[R1, E1, A1] = {
//    println(s"concat with $that")
//    new PushStream[R, E, A1] {
//      override def subscribe[OutR2 <: R, OutE2 >: E](observer: Observer[OutR2, OutE2, A1]): ZIO[OutR2, OutE2, Unit] = {
//        println(s"subscribe from $observer")
//        self.subscribe(new Observer[OutR2, OutE2, A1] {
//          override def onNext(elem: A1): ZIO[OutR2, OutE2, Ack] = observer.onNext(elem)
//
//          override def onComplete(): ZIO[OutR2, OutE2, Unit] = that.subscribe(observer)
//        })
//      }
//    }
//  }

  def runFold[B](z: B)(f: (B, A) => B): ZIO[R,E,B] = {
    Promise.make[Nothing, B].flatMap { completion =>
      var zState = z
      val stream: ZIO[R, E, Unit] = this.subscribe(new Observer[R, E, A] {
        override def onNext(elem: A): ZIO[R,E, Ack] = {
//          ZIO.succeed {
//            zState = f(zState, elem)
//          }.as(Continue)

          zState = f(zState, elem)
          Acks.Continue
        }

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

class LiftByOperatorPushStream[InR, InE, InA, OutR <: InR, OutE >: InE, OutB](upstream: PushStream[InR, InE, InA], operator: Operator[InA, OutR, OutE, OutB]) extends PushStream[OutR, OutE, OutB] {
//  override def subscribe(observer: Observer[OutR, OutE, OutB]): UIO[Unit]= {
//    operator(observer).flatMap { subscriberB =>
//      upstream.subscribe(subscriberB)
//    }
//  }

//  override def subscribe(observer: Observer[OutR, OutE, OutB]): ZIO[OutR, OutE, Unit] = ???

//  override def subscribe[OutR2 <: OutR, OutE2 >: OutE](observer: Observer[OutR2, OutE2, OutB]): ZIO[OutR2, OutE2, Unit] = {
  override def subscribe[OutR2 <: OutR, OutE2 >: OutE](observer: Observer[OutR2, OutE2, OutB]): ZIO[OutR2, OutE2, Unit] = {
    val transformedObserver: UIO[Observer[OutR2, OutE2, InA]] = operator(observer)

    val subscribedObserver: ZIO[OutR2, OutE2, Unit] = transformedObserver
      .flatMap { subscriberB =>
      upstream.subscribe(subscriberB)
    }
    subscribedObserver
  }
}

//class MapOperator[R, E, -A, +B](f: A => B) extends Operator[A, R,E,B]{
class MapOperator[R, E, A, B](f: A => B) extends Operator[A,R,E,B]{
  override def apply[OutR1 <: R, OutE1 >: E](out: Observer[OutR1, OutE1, B]): UIO[Observer[OutR1, OutE1, A]] = ZIO.succeed(new Observer[OutR1,OutE1,A] {
        override def onNext(elem: A): ZIO[OutR1, OutE1,Ack] = {
          out.onNext(f(elem))
        }

        override def onComplete(): UIO[Unit] = out.onComplete()
      })


}
//
//class MapParallelZioOperator[-A, +B](parallelism: Int, f: A => UIO[B]) extends Operator[A,B] {
//  def apply(out: Observer[B]): UIO[Observer[A]] = {
//    TSemaphore.makeCommit(parallelism).flatMap { permits =>
//      Queue.bounded[Fiber[Nothing, B]](parallelism * 2).flatMap { buffer =>
//        var stop = false
//        val consumer = new Observer[A] {
//          override def onNext(elem: A): UIO[Ack] = {
//            if (stop) Acks.Stop
//            else
//              for {
//                _ <- permits.acquire.commit
//                fiber <- f(elem).fork
//                _ <- buffer.offer(fiber)
//              } yield Ack.Continue
//          }
//
//          override def onComplete(): UIO[Unit] = {
//            permits.acquireN(parallelism).commit *> out.onComplete()
//          }
//        }
//
//        def take(): ZIO[Any, Throwable, Unit] = {
//          val runOnce = for {
//            fiber <- buffer.take
//            fiberResult <- fiber.join
//            _ <- permits.release.commit
//            _ <- out.onNext(fiberResult)
//          } yield ()
//
//          runOnce *> take()
//        }
//
////        def take2(): ZIO[Any, Throwable, Unit] = {
////          val runOnce = for {
////            fiber <- buffer.take
////            fiberResult <- fiber.await
////            _ <- permits.release.commit
////            _ <- fiberResult match {
////              case Exit.Success(value) => out.onNext(value) // TODO handle response
////              case Exit.Failure(cause) => ZIO.die(new RuntimeException("unhandled"))
////            }
////          } yield ()
////
////          runOnce *> take2()
////        }
//
//        val x: ZIO[Any, Nothing, Observer[A]] = for {
//          _ <- take().fork
//        } yield consumer
//
//        x
//      }
//    }
//  }
//}
//
//class MapParallelZioOperatorFast[-A, +B](parallelism: Int, f: A => UIO[B]) extends Operator[A,B] {
//  def apply(out: Observer[B]): UIO[Observer[A]] = {
//    val permits = new JSemaphore(parallelism)
//    val buffer = new ConcurrentLinkedQueue[Fiber[Nothing, B]]
//    var stop = false
//    val consumer = new Observer[A] {
//      override def onNext(elem: A): Task[Ack] = {
//        if (stop) Acks.Stop
//        else {
//          permits.acquire()
//          f(elem).fork.map { fiber =>
//            buffer.offer(fiber)
//            Ack.Continue
//          }
//        }
//      }
//
//      override def onComplete(): UIO[Unit] = {
//        permits.acquire(parallelism)
//        out.onComplete()
//      }
//    }
//
//    def take(): ZIO[Any, Throwable, Unit] = {
//      if (!buffer.isEmpty) {
//        val fiber = buffer.poll()
//
//        val runOnce = fiber.await.flatMap {
//          case Exit.Success(value) =>
//            permits.release()
//            out.onNext(value) // TODO handle response
//          case Exit.Failure(cause) =>
//            permits.release()
//            ZIO.die(new RuntimeException("unhandled"))
//        }
//
//        runOnce *> take()
//      } else {
//        ZIO.unit *> take()
//      }
//    }
//
//    val x: ZIO[Any, Nothing, Observer[A]] = for {
//      _ <- take().fork
//    } yield consumer
//
//    x
//  }
//}

class MapZioOperator[InA, OutR, OutE, OutB](f: InA => ZIO[OutR,OutE,OutB]) extends Operator[InA, OutR,OutE,OutB] {
  def apply[OutR1 <: OutR, OutE1 >: OutE](out: Observer[OutR1,OutE1,OutB]): UIO[Observer[OutR1,OutE1,InA]] = ZIO.succeed(new Observer[OutR1,OutE1,InA] {

    override def onNext(elem: InA): ZIO[OutR1,OutE1, Ack] = {
      val result: ZIO[OutR1, OutE1, Ack] = f(elem).flatMap { b =>
        out.onNext(b)
      }
      result
    }

    override def onComplete(): UIO[Unit] = out.onComplete()

  })
}
//
//class TakeOperator[T](n: Long) extends Operator[T,T] {
//  override def apply(out: Observer[T]): UIO[Observer[T]] = Ref.make(0).map { ref =>
//    new Observer[T] {
//      override def onNext(elem: T): Task[Ack] = {
//        ref.updateAndGet(i => i + 1).flatMap { emitted =>
//          if (emitted > n) out.onComplete().as(Stop) else out.onNext(elem)
//        }
//      }
//
//      override def onComplete(): UIO[Unit] = out.onComplete()
//    }
//  }
//}
//
//class TakeOperator2[T](n: Long) extends Operator[T,T] {
//  override def apply(out: Observer[T]): UIO[Observer[T]] = ZIO.succeed(new Observer[T] {
//    var ref: Long = 0L
//    override def onNext(elem: T): Task[Ack] = {
//      ref =ref + 1
//
//      if (ref > n) out.onComplete().as(Stop) else out.onNext(elem)
//    }
//
//    override def onComplete(): UIO[Unit] = out.onComplete()
//  })
//}

object PushStream {
//  type PureOperator[-I, +O] = Observer[O] => UIO[Observer[I]]

  trait Operator[InA, OutR, OutE, +OutB] {
    def apply[OutR1 <: OutR, OutE1 >: OutE](observer: Observer[OutR1, OutE1, OutB]): UIO[Observer[OutR1, OutE1, InA]]
  }

  def apply[T](elems: T*): PushStream[Any, Nothing, T] = {
    fromIterable(elems)
  }

  def fromIterable[T](elems: Iterable[T]): PushStream[Any, Nothing, T] = {
    new PushStream[Any, Nothing, T] {
      override def subscribe[OutR, OutE](observer: Observer[OutR, OutE, T]): ZIO[OutR, OutE, Unit] = {
        val iterator = elems.iterator

        def emit(): ZIO[OutR, OutE, Unit] = {
          val hasNext = iterator.hasNext

          if (hasNext) observer.onNext(iterator.next()) *> emit()
          else observer.onComplete()
        }

        emit()
      }
    }
  }

  def range(start: Int, end: Int): PushStream[Any, Nothing, Int] = {
    new PushStream[Any, Nothing, Int] {

//      override def subscribe(observer: Observer[Any, Nothing, Int]): UIO[Unit] = {
//        loop(start, observer)
//      }
      override def subscribe[OutR2 <: Any, OutE2 >: Nothing](observer: Observer[OutR2, OutE2, Int]): ZIO[OutR2, OutE2, Unit] = {
        loop(start, observer)
      }

      private def asyncAck[R, E](next:Int, observer: Observer[R, E, Int], ack: ZIO[R, E, Ack]): ZIO[R, E, Unit] = {
        ack.flatMap {
          case Stop => ZIO.unit
          case Continue => loop(next + 1, observer)
        }
      }
//      @tailrec
      private def loop[R, E](next:Int, observer: Observer[R, E, Int]): ZIO[R,E, Unit] = {
        if (next >= end) observer.onComplete()
        else {
          val ack = observer.onNext(next)

//          if (ack == Acks.Stop) ZIO.unit
//          else if (ack == Acks.Continue)
//            loop(next + 1, observer)
//          else
            asyncAck(next, observer, ack)
        }
      }


    }
  }
}

object Example extends ZIOAppDefault {
  override def run: ZIO[Any with ZIOAppArgs with Scope, Any, Any] = {
    val semaphoreProg = TSemaphore.makeCommit(8).flatMap { permits =>
      ZIO.foreachDiscard(0 to 500_000) { i =>
        for {
          _ <- permits.acquire.commit
          _ <- permits.release.commit
        } yield ()
      }
    }

    val enqueueProg = Queue.bounded[Int](8 * 2).flatMap { buffer =>
      val enqueue = ZIO.foreachDiscard(0 to 500_000) { i =>
        buffer.offer(i)
      }

      val dequeue = ZIO.foreachDiscard(0 to 500_000){ _ =>
        buffer.take
      }

      enqueue &> dequeue
    }

    val fiberProg = for {
      sumFibers <- ZIO.foreach((0 to 500_000).toList) { i => ZIO.succeed(i * 2).fork }
      sums <- ZIO.foreach(sumFibers)(fiber => fiber.join)
    } yield sums.sum
    // .mapZio(a => ZIO.succeed(a *2).delay(1.millis)).take(5)

    val range: PushStream[Any, Nothing, Int] = PushStream.range(0, 500_000)
      val mapped: PushStream[Any, String, Int] = range.mapZIO(i => if (i == 5) ZIO.fail("dead") else ZIO.succeed(i * 2))
        val folded: ZIO[Any, String, Long] = mapped.runFold(0L)(_ + _)

    val streamMax = 5_000_000
    val pushStream = PushStream.range(0, streamMax)
    val zStream = ZStream.range(0, streamMax, 1)

    val program = for {
      _ <- ZIO.unit
      rangePS = pushStream.runFold(0L)(_ + _)
      rangeZS = zStream.runFold(0L)(_ + _)
      _ <- timed("ZS - range and fold", rangeZS)
      _ <- timed("PS - range and fold", rangePS)
      mapZS = zStream.map(i => i * 2).runFold(0L)(_ + _)
      mapPS = pushStream.map(i => i * 2).runFold(0L)(_ + _)
      _ <- timed("ZS - map", mapZS)
      _ <- timed("PS - map", mapPS)
      mapZioZS = zStream.mapZIO(i => ZIO.succeed(i * 2)).runFold(0L)(_ + _)
      mapZioPS = pushStream.mapZIO(i => ZIO.succeed(i * 2)).runFold(0L)(_ + _)
      _ <- timed("ZS - mapZio", mapZioZS)
      _ <- timed("PS - mapZio", mapZioPS)
//      mapParResult = PushStream.range(0, 500_000).mapZioPar(8)(i => ZIO.succeed(i*2)).runFold(0L)(_ + _)
//      _ <- timed("mapParResult", mapParResult)
//      result <- fiberProg.timed
//     result2 <- PushStream.range(0, 100000).mapZioParFast(8)(i => ZIO.succeed(i * 2)).runFold(0L)(_ + _).timed
//     _ <- ZIO.succeed(println(s"${result2._2} took ${result2._1.toMillis}"))
    } yield ()

    program //.repeatN(5)

//    (PushStream.range(1,4) ++ PushStream.range(5, 10)).mapZio(i => zio.Console.printLine(s"got $i").orDie).runCollect
//
//    SamplingProfiler().profile(program).flatMap(_.stackCollapseToFile("profile.folded"))
  }

  private def timed[R,E,A](description: String, task: ZIO[R, E, A]): ZIO[R, E, Unit] = {
    val iterations = 5
    val timedTask = for {
      result <- task.timed
      //      _ <- ZIO.sleep(1.second)
    } yield result

    val results = ZIO.foreach((1 to iterations).toList)(_ => timedTask)
    results.flatMap { results =>
      val time = results.map(_._1.toMillis).sum / iterations
      val result = results.head._2
      ZIO.succeed(println(s"$description took ${time}ms on average over $iterations iterations to calculate $result"))
    }
  }
}