package zio.streams.push

import zio.{Chunk, ZIO}
import zio.test.Assertion._
import zio.test.TestAspect.{exceptJS, flaky, nonFlaky, scala2Only, withLiveClock}
import zio.test._

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
//      // Chunking is not a concern.
//      test("range emits values in chunks of chunkSize") {
//        assertZIO(
//          (PushStream
//            .range(1, 10, 2))
//            .mapChunks(c => Chunk[Int](c.sum))
//            .runCollect
//        )(equalTo(Chunk(1 + 2, 3 + 4, 5 + 6, 7 + 8, 9)))
//      }
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
    )
  )
}
