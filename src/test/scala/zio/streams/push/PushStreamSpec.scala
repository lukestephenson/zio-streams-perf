package zio.streams.push

import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect.nonFlaky
import zio.{Chunk, Clock, Promise, Queue, Ref, Schedule, Scope, ZIO, durationInt}
import zio._

import java.util.concurrent.TimeUnit

object PushStreamSpec extends ZIOSpecDefault {

  def pureStreamGen[R, A](a: Gen[R, A], max: Int): Gen[R, PushStream[Any, Nothing, A]] =
    max match {
      case 0 => Gen.const(PushStream.empty)
      case n =>
        Gen.oneOf(
          Gen.const(PushStream.empty),
          Gen
            .int(1, n)
            .flatMap(Gen.listOfN(_)(a))
            .map(elements => PushStream.fromIterable(elements))
        )
    }

  val pureStreamOfInts: Gen[Any, PushStream[Any, Nothing, Int]] =
    Gen.bounded(0, 5)(pureStreamGen(Gen.int, _)) // .zipWith(Gen.function(Gen.boolean))(injectEmptyChunks)

  override def spec = suite("PushStreamSpec")(
    suite("mapZIO")(
      test("ZIO#foreach equivalence") {
        check(Gen.small(Gen.listOfN(_)(Gen.byte)), Gen.function(Gen.successes(Gen.byte))) { (data, f) =>
          val s = PushStream.fromIterable(data)

          for {
            l <- s.mapZIO(f).runCollect
            r <- ZIO.foreach(data)(f)
          } yield assert(l.toList)(equalTo(r))
        }
      }
//      test("laziness on chunks") {
//        assertZIO(
//          PushStream(1, 2, 3).mapZIO {
//            case 3 => ZIO.fail("boom")
//            case x => ZIO.succeed(x)
//          }.either.runCollect
//        )(equalTo(Chunk(Right(1), Right(2), Left("boom"))))
//      }
    ),
    suite("mapZIOPar")(
      test("foreachParN equivalence") {
        checkN(10)(Gen.small(Gen.listOfN(_)(Gen.byte)), Gen.function(Gen.successes(Gen.byte))) { (data, f) =>
          val s = PushStream.fromIterable(data)

          for {
            l <- s.mapZIOPar(8)(f).runCollect
            r <- ZIO.foreachPar(data)(f).withParallelism(8)
          } yield assert(l.toList)(equalTo(r))
        }
      },
      test("order when n = 1") {
        for {
          queue <- Queue.unbounded[Int]
          _ <- PushStream.range(0, 9).mapZIOPar(1)(queue.offer).runDrain
          result <- queue.takeAll
        } yield assert(result)(equalTo(result.sorted))
      },
      test("interruption propagation") {
        for {
          interrupted <- Ref.make(false)
          latch <- Promise.make[Nothing, Unit]
          fib <- PushStream(())
            .mapZIOPar(1)(_ => (latch.succeed(()) *> ZIO.infinity).onInterrupt(interrupted.set(true)))
            .runDrain
            .fork
          _ <- latch.await
          _ <- fib.interrupt
          result <- interrupted.get
        } yield assert(result)(isTrue)
      },
      test("guarantee ordering")(check(Gen.int(1, 4096), Gen.listOf(Gen.int)) { (n: Int, m: List[Int]) =>
        for {
          mapZIO <- PushStream.fromIterable(m).mapZIO(ZIO.succeed(_)).runCollect
          mapZIOPar <- PushStream.fromIterable(m).mapZIOPar(n)(ZIO.succeed(_)).runCollect
        } yield assert(n)(isGreaterThan(0)).implies(assert(mapZIO)(equalTo(mapZIOPar)))
      }),
      test("awaits children fibers properly") {
        assertZIO(
          PushStream
            .fromIterable(0 to 100)
//            .interruptWhen(ZIO.never) // TODO why does the ZStream spec have this?
            .mapZIOPar(8)(_ => ZIO.succeed(1).repeatN(2000))
            .runDrain
            .exit
            .map(_.isInterrupted)
        )(equalTo(false))
      },
      test("interrupts pending tasks when one of the tasks fails") {
        for {
          interrupted <- Ref.make(0)
          latch1 <- Promise.make[Nothing, Unit]
          latch2 <- Promise.make[Nothing, Unit]
          result <- PushStream(1, 2, 3)
            .mapZIOPar(3) {
              case 1 => (latch1.succeed(()) *> ZIO.never).onInterrupt(interrupted.update(_ + 1))
              case 2 => (latch2.succeed(()) *> ZIO.never).onInterrupt(interrupted.update(_ + 1))
              case 3 => latch1.await *> latch2.await *> ZIO.fail("Boom")
            }
            .runDrain
            .exit
          // TODO modified from the original ZStream spec to sleep for 100ms before checking the interrupted count
          count <- ZIO.succeed(Thread.sleep(100)) *> interrupted.get
        } yield assert(count)(equalTo(2)) && assert(result)(fails(equalTo("Boom")))
      } @@ nonFlaky,
      test("propagates correct error with subsequent mapZIOPar call (#4514)") {
        assertZIO(
          PushStream
            .fromIterable(1 to 50)
            .mapZIOPar(20, "first")(i => if (i < 10) ZIO.succeed(i) else ZIO.fail("Boom"))
            .mapZIOPar(20, "second")(ZIO.succeed(_))
            .runCollect
            .either
        )(isLeft(equalTo("Boom")))
      } @@ nonFlaky,
      test("propagates error of original stream") {
        for {
          fiber <- (PushStream(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) ++ PushStream.fail(new Throwable("Boom")))
            .mapZIOPar(2)(_ => ZIO.sleep(1.second))
            .runDrain
            .fork
          _ <- TestClock.adjust(5.seconds)
          exit <- fiber.await
        } yield assert(exit)(fails(hasMessage(equalTo("Boom"))))
      }
    ),
    suite("scan")(
      test("scan")(check(pureStreamOfInts) { s =>
        for {
          streamResult <- s.scan(0)(_ + _).runCollect
          chunkResult <- s.runCollect.map(_.scan(0)(_ + _))
        } yield assert(streamResult)(equalTo(chunkResult))
      })
    ),
    suite("schedule")(
      test("schedule") {
        for {
          start <- Clock.currentTime(TimeUnit.MILLISECONDS)
          fiber <- PushStream
            .range(1, 9)
            .schedule(Schedule.fixed(100.milliseconds))
            .mapZIO(n => Clock.currentTime(TimeUnit.MILLISECONDS).map(now => (n, now - start)))
            .runCollect
            .fork
          _ <- TestClock.adjust(800.millis)
          actual <- fiber.join
          expected = Chunk((1, 100L), (2, 200L), (3, 300L), (4, 400L), (5, 500L), (6, 600L), (7, 700L), (8, 800L))
        } yield assertTrue(actual == expected)
      },
      test("scheduleWith")(
        assertZIO(
          PushStream("A", "B", "C", "A", "B", "C")
            .scheduleWith(Schedule.recurs(2) *> Schedule.fromFunction(_ => "Done"))(
              _.toLowerCase,
              identity
            )
            .runCollect
        )(equalTo(Chunk("a", "Done", "b", "Done"))) // Note this differs to ZStream assertion
      ),
      test("scheduleEither")(
        assertZIO(
          PushStream("A", "B", "C")
            .scheduleEither(Schedule.recurs(2) *> Schedule.fromFunction(_ => "!"))
            .runCollect
        )(equalTo(Chunk(Right("A"), Left("!"), Right("B"), Left("!")))) // Note this differs to ZStream assertion
      )
    ),
    suite("repeatEffectOption")(
      test("emit elements")(
        assertZIO(
          PushStream
            .repeatZIOOption(ZIO.succeed(1))
            .take(2)
            .runCollect
        )(equalTo(Chunk(1, 1)))
      ),
      test("emit elements until pull fails with None")(
        for {
          ref <- Ref.make(0)
          fa = for {
            newCount <- ref.updateAndGet(_ + 1)
            res <- if (newCount >= 5) ZIO.fail(None) else ZIO.succeed(newCount)
          } yield res
          res <- PushStream
            .repeatZIOOption(fa)
            .take(10)
            .runCollect
        } yield assert(res)(equalTo(Chunk(1, 2, 3, 4)))
      )
//      test("stops evaluating the effect once it fails with None") {
//        for {
//          ref <- Ref.make(0)
//          _ <- ZIO.scoped {
//            PushStream.repeatZIOOption(ref.updateAndGet(_ + 1) *> ZIO.fail(None)).toPull.flatMap { pull =>
//              pull.ignore *> pull.ignore
//            }
//          }
//          result <- ref.get
//        } yield assert(result)(equalTo(1))
//      }
    ),
    suite("Constructors")(
      suite("range")(
        test("range includes min value and excludes max value") {
          assertZIO(
            PushStream.range(1, 2).runCollect
          )(equalTo(Chunk(1)))
        },
        test("two large ranges can be concatenated") {
          assertZIO(
            (PushStream.range(1, 1000) ++ PushStream.range(1000, 2000)).runCollect
          )(equalTo(Chunk.fromIterable(Range(1, 2000))))
        },
        test("two small ranges can be concatenated") {
          assertZIO(
            (PushStream.range(1, 10) ++ PushStream.range(10, 20)).runCollect
          )(equalTo(Chunk.fromIterable(Range(1, 20))))
        },
        test("range emits no values when start >= end") {
          assertZIO(
            (PushStream.range(1, 1) ++ PushStream.range(2, 1)).runCollect
          )(equalTo(Chunk.empty))
        }
      ),
      test("unwrapScoped") {
        def stream(promise: Promise[Nothing, Unit]) =
          PushStream.unwrapScoped {
            val scoped = ZIO.acquireRelease(Console.print("acquire outer"))(_ => Console.print("release outer").orDie) *>
              ZIO.suspendSucceed(promise.succeed(()) *> ZIO.never) *>
              ZIO.succeed(PushStream(1, 2, 3))
            scoped
          }
        for {
          promise <- Promise.make[Nothing, Unit]
          fiber <- stream(promise).runDrain.fork
          _ <- promise.await
          _ <- fiber.interrupt
          output <- TestConsole.output
        } yield assertTrue(output == Vector("acquire outer", "release outer"))
      }
    )
  )
}
