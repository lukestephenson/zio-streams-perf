package zio.streams.push

import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect.nonFlaky
import zio.{Chunk, Clock, Promise, Queue, Ref, Schedule, Scope, ZIO, durationInt}
import zio._

import java.util.concurrent.TimeUnit

object PushStreamSpec extends ZIOSpecDefault with GenZIO {

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
//
//  def failingStreamGen[R <: Random, A](a: Gen[R, A], max: Int): Gen[R with Sized, PushStream[Any, String, A]] =
//    max match {
//      case 0 => Gen.const(PushStream.fromZIO(ZIO.fail("fail-case")))
//      case _ =>
//        Gen
//          .int(1, max)
//          .flatMap(n =>
//            for {
//              i <- Gen.int(0, n - 1)
//              it <- Gen.listOfN(n)(a)
//            } yield ZStream.unfoldM((i, it)) {
//              case (_, Nil) | (0, _) => IO.fail("fail-case")
//              case (n, head :: rest) => IO.succeedNow(Some((head, (n - 1, rest))))
//            }
//          )
//    }

  def streamGen[R <: Random, A](a: Gen[R, A], max: Int): Gen[R with Sized, PushStream[Any, String, A]] =
//    Gen.oneOf(failingStreamGen(a, max), pureStreamGen(a, max))
    Gen.oneOf(pureStreamGen(a, max))

  val streamOfInts: Gen[Random with Sized, PushStream[Any, String, Int]] =
    Gen.bounded(0, 5)(streamGen(Gen.int, _)) // .zipWith(Gen.function(Gen.boolean))(injectEmptyChunks)

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
//            .interruptWhen(ZIO.never) // TODO why does the PushStream spec have this?
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
          // TODO modified from the original PushStream spec to sleep for 100ms before checking the interrupted count
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
        )(equalTo(Chunk("a", "Done", "b", "Done"))) // Note this differs to PushStream assertion
      ),
      test("scheduleEither")(
        assertZIO(
          PushStream("A", "B", "C")
            .scheduleEither(Schedule.recurs(2) *> Schedule.fromFunction(_ => "!"))
            .runCollect
        )(equalTo(Chunk(Right("A"), Left("!"), Right("B"), Left("!")))) // Note this differs to PushStream assertion
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
    suite("take")(
//      test("take")(check(streamOfInts, Gen.int) { (s, n) =>
//        for {
//          takeStreamResult <- s.take(n).runCollect.exit
//          takeListResult   <- s.runCollect.map(_.take(n)).exit
//        } yield assert(takeListResult.isSuccess)(isTrue) implies assert(takeStreamResult)(
//          equalTo(takeListResult)
//        )
//      }),
//      test("take short circuits")(
//        for {
//          ran    <- Ref.make(false)
//          stream  = (PushStream(1) ++ PushStream.fromZIO(ran.set(true).as(2))).take(0)
//          _      <- stream.runDrain
//          result <- ran.get
//        } yield assert(result)(isFalse)
//      ),
      test("take(0) short circuits")(
        for {
          units <- PushStream.never.take(0).runCollect
        } yield assert(units)(equalTo(Chunk.empty))
      ) @@ TestAspect.timeout(1.second),
      test("take(1) short circuits")(
        for {
          ints <- (PushStream(1) ++ PushStream.never).take(1).runCollect
        } yield assert(ints)(equalTo(Chunk(1)))
      ) // @@ TestAspect.timeout(1.second)
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
    ),
    suite("acquireReleaseWith")(
      test("acquireReleaseWith")(
        for {
          done <- Ref.make(false)
          iteratorStream = PushStream
            .acquireReleaseWith(ZIO.succeed(0 to 2))(_ => done.set(true))
            .flatMap(PushStream.fromIterable(_))
          result <- iteratorStream.runCollect
          released <- done.get
        } yield assert(result)(equalTo(Chunk(0, 1, 2))) && assert(released)(isTrue)
      ),
      test("acquireReleaseWith short circuits")(
        for {
          done <- Ref.make(false)
          iteratorStream = PushStream
            .acquireReleaseWith(ZIO.succeed(0 to 3))(_ => done.set(true))
            .flatMap(PushStream.fromIterable(_))
            .take(2)
          result <- iteratorStream.runCollect
          released <- done.get
        } yield assert(result)(equalTo(Chunk(0, 1))) && assert(released)(isTrue)
      ),
      test("no acquisition when short circuiting")(
        for {
          acquired <- Ref.make(false)
          iteratorStream = (PushStream(1) ++ PushStream.acquireReleaseWith(acquired.set(true))(_ => ZIO.unit))
            .take(0)
          _ <- iteratorStream.runDrain
          result <- acquired.get
        } yield assert(result)(isFalse)
      ),
      test("releases when there are defects") {
        for {
          ref <- Ref.make(false)
          _ <- PushStream
            .acquireReleaseWith(ZIO.unit)(_ => ref.set(true))
            .flatMap(_ => PushStream.fromZIO(ZIO.dieMessage("boom")))
            .runDrain
            .exit
          released <- ref.get
        } yield assert(released)(isTrue)
      },
      test("flatMap associativity doesn't affect acquire release lifetime")(
        for {
          leftAssoc <- PushStream
            .acquireReleaseWith(Ref.make(true))(_.set(false))
            .flatMap(PushStream(_))
            .flatMap(r => PushStream.fromZIO(r.get))
            .runCollect
            .map(_.head)
          rightAssoc <- PushStream
            .acquireReleaseWith(Ref.make(true))(_.set(false))
            .flatMap(PushStream(_).flatMap(r => PushStream.fromZIO(r.get)))
            .runCollect
            .map(_.head)
        } yield assert(leftAssoc -> rightAssoc)(equalTo(true -> true))
      ),
      test("propagates errors") {
        val stream = PushStream.acquireReleaseWith(ZIO.unit)(_ => ZIO.dieMessage("die"))
        assertZIO(stream.runCollect.exit)(dies(anything))
      }
    ),
    suite("flatMap")(
      test("deep flatMap stack safety") {
        println("stack safety")
        def fib(n: Int): PushStream[Any, Nothing, Int] =
          if (n <= 1) PushStream.succeed(n)
          else
            fib(n - 1).flatMap(a => fib(n - 2).flatMap(b => PushStream.succeed(a + b)))

        val stream = fib(20)
        val expected = 6765

        assertZIO(stream.runCollect)(equalTo(Chunk(expected)))
      } @@ TestAspect.jvmOnly, // Too slow on Scala.js
      test("left identity")(check(Gen.int, Gen.function(pureStreamOfInts)) { (x, f) =>
        for {
          res1 <- PushStream(x).flatMap(f).runCollect
          res2 <- f(x).runCollect
        } yield assert(res1)(equalTo(res2))
      }),
      test("right identity")(
        check(pureStreamOfInts)(m =>
          for {
            res1 <- m.flatMap(i => PushStream(i)).runCollect
            res2 <- m.runCollect
          } yield assert(res1)(equalTo(res2))
        )
      ),
      test("associativity") {
        val tinyStream = Gen.int(0, 2).flatMap(pureStreamGen(Gen.int, _))
        val fnGen = Gen.function(tinyStream)
        check(tinyStream, fnGen, fnGen) { (m, f, g) =>
          for {
            leftStream <- m.flatMap(f).flatMap(g).runCollect
            rightStream <- m.flatMap(x => f(x).flatMap(g)).runCollect
          } yield assert(leftStream)(equalTo(rightStream))
        }
      } @@ TestAspect.jvmOnly, // Too slow on Scala.js
      test("inner finalizers") {
        for {
          effects <- Ref.make(List[Int]())
          push = (i: Int) => effects.update(i :: _)
          latch <- Promise.make[Nothing, Unit]
          fiber <- PushStream(
            PushStream.acquireReleaseWith(push(1))(_ => push(1)),
            PushStream.fromZIO(push(2)),
            PushStream.acquireReleaseWith(push(3))(_ => push(3)) *> PushStream.fromZIO(
              latch.succeed(()) *> ZIO.never
            )
          ).flatMap(identity).runDrain.fork
          _ <- latch.await
          _ <- fiber.interrupt
          result <- effects.get
        } yield assert(result)(equalTo(List(3, 3, 2, 1, 1)))
      },
      test("finalizer ordering 1") {
        for {
          effects <- Ref.make(List[String]())
          push = (i: String) => effects.update(i :: _)
          stream = for {
            _ <- PushStream.acquireReleaseWith(push("open1"))(_ => push("close1"))
            _ <- PushStream.apply((), ())
              // .fromChunks(Chunk(()), Chunk(()))
              .tap(_ => ZIO.debug("use2") *> push("use2"))
              .ensuring(push("close2"))
            _ <- PushStream.acquireReleaseWith(ZIO.debug("open3") *> push("open3"))(_ =>
              ZIO.debug("close3") *> push("close3")
            )
            _ <- PushStream.apply((), ())
//              .fromChunks(Chunk(()), Chunk(()))
              .tap(_ => push("use4"))
              .ensuring(push("close4"))
          } yield ()
          _ <- stream.runDrain
          result <- effects.get
        } yield assert(result.reverse)(
          equalTo(
            List(
              "open1",
              "use2",
              "open3",
              "use4",
              "use4",
              "close4",
              "close3",
              "use2",
              "open3",
              "use4",
              "use4",
              "close4",
              "close3",
              "close2",
              "close1"
            )
          )
        )
      },
      test("finalizer ordering 2") {
        for {
          effects <- Ref.make(List[String]())
          push = (i: String) => effects.update(i :: _)
          stream = for {
            _ <- PushStream(1, 2)
//              .fromChunks(Chunk(1), Chunk(2))
              .tap(n => ZIO.debug(s">>> use2 $n") *> push("use2"))
            _ <- PushStream.acquireReleaseWith(ZIO.debug("open3") *> push("open3"))(_ =>
              ZIO.debug("close3") *> push("close3")
            )
          } yield ()
          _ <- stream.runDrain
          result <- effects.get
        } yield assert(result.reverse)(
          equalTo(
            List(
              "use2",
              "open3",
              "close3",
              "use2",
              "open3",
              "close3"
            )
          )
        )
      }
//      test("exit signal") {
//        for {
//          ref <- Ref.make(false)
//          inner = PushStream
//            .acquireReleaseExitWith(ZIO.unit)((_, e) =>
//              e match {
//                case Exit.Failure(_) => ref.set(true)
//                case Exit.Success(_) => ZIO.unit
//              }
//            )
//            .flatMap(_ => PushStream.fail("Ouch"))
//          _   <- PushStream.succeed(()).flatMap(_ => inner).runDrain.either.unit
//          fin <- ref.get
//        } yield assert(fin)(isTrue)
//      },
//      test("finalizers are registered in the proper order") {
//        for {
//          fins <- Ref.make(List[Int]())
//          s = PushStream.finalizer(fins.update(1 :: _)) *>
//            PushStream.finalizer(fins.update(2 :: _))
//          _      <- ZIO.scoped[Any](s.toPull.flatten)
//          result <- fins.get
//        } yield assert(result)(equalTo(List(1, 2)))
//      },
//      test("early release finalizer concatenation is preserved") {
//        for {
//          fins <- Ref.make(List[Int]())
//          s = PushStream.finalizer(fins.update(1 :: _)) *>
//            PushStream.finalizer(fins.update(2 :: _))
//          result <- Scope.make.flatMap { scope =>
//            scope.extend(s.toPull).flatMap { pull =>
//              pull *> scope.close(Exit.unit) *> fins.get
//            }
//          }
//        } yield assert(result)(equalTo(List(1, 2)))
//      }
    ),
    test("ensuring") {
      for {
        log <- Ref.make[List[String]](Nil)
        _ <- (for {
          _ <- PushStream.acquireReleaseWith(log.update("Acquire" :: _))(_ => log.update("Release" :: _))
          _ <- PushStream.fromZIO(log.update("Use" :: _))
        } yield ()).ensuring(log.update("Ensuring" :: _)).runDrain
        execution <- log.get
      } yield assert(execution)(equalTo(List("Ensuring", "Release", "Use", "Acquire")))
    }
  )
}
