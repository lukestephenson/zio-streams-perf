package zio.streams.push.benchmarks

import org.openjdk.jmh.annotations.*
import zio.ZIO
import zio.stream.ZStream
import zio.streams.push.ChunkedPushStream.*
import zio.streams.push.{ChunkedPushStream, PushStream}

class MapZioBenchmark extends BaseBenchmark {

  @Benchmark
  def PushStreamChunk1() = {
    runZIO(PushStream.range(0, 1_000_000).mapZIO(i => ZIO.succeed(i * 2)).runFold(0)(_ + _))
  }

  @Benchmark
  def ZStreamChunk1() = {
    runZIO(ZStream.range(0, 1_000_000, 1).mapZIO(i => ZIO.succeed(i * 2)).runFold(0)(_ + _))
  }

  @Benchmark
  def PushStreamChunk100() = {
    runZIO(ChunkedPushStream.range(0, 1_000_000, 100).mapZIOChunks(i => ZIO.succeed(i * 2)).runFold(0)(_ + _.sum))
  }

  @Benchmark
  def ZStreamChunk100() = {
    runZIO(ZStream.range(0, 1_000_000, 100).mapZIO(i => ZIO.succeed(i * 2)).runFold(0)(_ + _))
  }

}
