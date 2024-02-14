package zio.streams.push

import zio.test.*
import zio.test.Assertion.*
import zio.test.TestAspect.nonFlaky
import zio.{Chunk, Promise, Queue, Ref, ZIO, durationInt}

object PushStreamSpec extends ZIOSpecDefault {

  override def spec = suite("PushStreamSpec")(
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
              case 1 => (latch1.succeed(()) *> ZIO.never *> zio.Console.printLine("not expected")).onInterrupt(
                  zio.Console.printLine("1 - interrupted").either *> interrupted.update(_ + 1)
                )
              case 2 =>
                (latch2.succeed(()) *> ZIO.never).onInterrupt(zio.Console.printLine("2 - interrupted").either *> interrupted.update(_ + 1))
              case 3 => latch1.await *> latch2.await *> zio.Console.printLine("3 - about to fail") *> ZIO.fail("Boom")
            }
            .runDrain
            .exit
          _ <- zio.Console.printLine("push stream has finished").ignore
          count <- interrupted.get
        } yield assert(count)(equalTo(2)) && assert(result)(fails(equalTo("Boom")))
      } @@ nonFlaky,
      test("propagates correct error with subsequent mapZIOPar call (#4514)") {
        assertZIO(
          PushStream
            .fromIterable(1 to 50)
            .mapZIOPar(20)(i => if (i < 10) ZIO.succeed(i) else ZIO.fail("Boom"))
            .mapZIOPar(20)(ZIO.succeed(_))
            .runCollect
            .either
        )(isLeft(equalTo("Boom")))
      } @@ nonFlaky,
      test("propagates error of original stream") {
        for {
          fiber <- (PushStream(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) ++ PushStream.fail(new Throwable("Boom"))) // */
            .mapZIOPar(2)(i => ZIO.sleep(1.second) *> zio.Console.printLine(s"done $i"))
            .runDrain
            .fork
          _ <- TestClock.adjust(5.seconds)
          _ <- zio.Console.printLine("waiting").ignore
          exit <- fiber.await
        } yield assert(exit)(fails(hasMessage(equalTo("Boom"))))
      }
    )
  )
}

// LiftByOperatorPushStream - subscribe
//SourcePushStream2 - subscribe
//fold picked up a failure
//SourcePushStream2 done emitting elements
//SourcePushStream2 - Success(())
//LiftByOperatorPushStream - Success(())
//runFold stream Success(())
//stream has terminated
//runFold Failure(Fail(java.lang.Throwable: Boom,Stack trace for thread "zio-fiber-77":
//	at zio.streams.push.PushStream.runFold.stream.$anon.onError(PushStream.scala:71)
//	at zio.streams.push.PushStream.runFold.x(PushStream.scala:83)
//	at zio.streams.push.PushStream.runFold(PushStream.scala:85)
//	at zio.streams.push.PushStreamSpec.spec(PushStreamSpec.scala:145)
//	at zio.streams.push.PushStreamSpec.spec(PushStreamSpec.scala:146)
//	at zio.streams.push.PushStreamSpec.spec(PushStreamSpec.scala:151)
//	at zio.streams.push.PushStreamSpec.spec(PushStreamSpec.scala:152)))
//waiting
