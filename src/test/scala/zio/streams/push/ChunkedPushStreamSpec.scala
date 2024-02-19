package zio.streams.push

import zio.streams.push.ChunkedPushStream.*
import zio.streams.push.PushStreamSpec.{suite, test}
import zio.test.*
import zio.test.Assertion.*
import zio.{Chunk, ZIO}

object ChunkedPushStreamSpec extends ZIOSpecDefault {

  override def spec = suite("ChunkedPushStreamSpec")(
    suite("range")(
      test("range includes min value and excludes max value") {
        assertZIO(
          ChunkedPushStream.range(1, 2, 10).runCollect
        )(equalTo(Chunk(Chunk(1))))
      },
      test("two large ranges can be concatenated") {
        assertZIO(
          (ChunkedPushStream.range(1, 1000, 10) ++ ChunkedPushStream.range(1000, 2000, 10)).runCollect.map(_.flatten)
        )(equalTo(Chunk.fromIterable(Range(1, 2000))))
      },
      test("two small ranges can be concatenated") {
        assertZIO(
          (ChunkedPushStream.range(1, 10, 10) ++ ChunkedPushStream.range(10, 20, 10)).runCollect.map(_.flatten)
        )(equalTo(Chunk.fromIterable(Range(1, 20))))
      },
      test("range emits no values when start >= end") {
        assertZIO(
          (ChunkedPushStream.range(1, 1, 10) ++ ChunkedPushStream.range(2, 1, 10)).runCollect.map(_.flatten)
        )(equalTo(Chunk.empty))
      },
      test("range emits values in chunks of chunkSize") {
        assertZIO(
          ChunkedPushStream
            .range(1, 10, 2)
            .map(c => c.sum)
            .runCollect
        )(equalTo(Chunk(1 + 2, 3 + 4, 5 + 6, 7 + 8, 9)))
      }
    )
  )
}
