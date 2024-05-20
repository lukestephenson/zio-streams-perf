package zio.streams.push.benchmarks

import monix.eval.Task as MonixTask
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.openjdk.jmh.annotations.*
import zio.stream.ZStream
import zio.streams.push.ChunkedPushStream.*
import zio.streams.push.{ChunkedPushStream, PushStream}

class MapBenchmark extends BaseBenchmark {

  @Benchmark
  def observable() = {
    Observable.range(0, StreamLength).map(_ * 2).foldLeftL(0L)(_ + _).runSyncUnsafe()
  }

  @Benchmark
  def PushStreamChunk1() = {
    runZIO(PushStream.range(0, StreamLength).map(_ * 2).runFold(0)(_ + _))
  }

  @Benchmark
  def ZStreamChunk1() = {
    runZIO(ZStream.range(0, StreamLength, 1).map(_ * 2).runFold(0)(_ + _))
  }

  @Benchmark
  def PushStreamChunk100() = {
    runZIO(ChunkedPushStream.range(0, StreamLength, 100).mapChunks(_ * 2).runFold(0)(_ + _.sum))
  }

  @Benchmark
  def ZStreamChunk100() = {
    runZIO(ZStream.range(0, StreamLength, 100).map(_ * 2).runFold(0)(_ + _))
  }
}
