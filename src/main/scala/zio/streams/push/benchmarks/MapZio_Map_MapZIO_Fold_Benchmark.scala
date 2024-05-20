package zio.streams.push.benchmarks

import monix.eval.Task as MonixTask
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.openjdk.jmh.annotations.*
import zio.ZIO
import zio.stream.ZStream
import zio.streams.push.ChunkedPushStream.*
import zio.streams.push.{ChunkedPushStream, PushStream}

class MapZio_Map_MapZIO_Fold_Benchmark extends BaseBenchmark {

  @Benchmark
  def observable() = {
    Observable.range(0, 1_000_000)
      .mapEval(i => MonixTask.now(i * 4))
      .map(i => i / 2)
      .mapEval(i => MonixTask.now(i / 2))
      .foldLeftL(0L)(_ + _).runSyncUnsafe()
  }

  @Benchmark
  def PushStreamChunk1() = {
    runZIO(PushStream.range(0, 1_000_000)
      .mapZIO(i => ZIO.succeed(i * 4))
      .map(i => i / 2)
      .mapZIO(i => ZIO.succeed(i / 2))
      .runFold(0)(_ + _))
  }

  @Benchmark
  def ZStreamChunk1() = {
    runZIO(ZStream.range(0, 1_000_000, 1)
      .mapZIO(i => ZIO.succeed(i * 4))
      .map(i => i / 2)
      .mapZIO(i => ZIO.succeed(i / 2))
      .runFold(0)(_ + _))
  }

  @Benchmark
  def PushStreamChunk100() = {
    runZIO(ChunkedPushStream.range(0, 1_000_000, 100)
      .mapZIOChunks(i => ZIO.succeed(i * 4))
      .mapChunks(i => i / 2)
      .mapZIOChunks(i => ZIO.succeed(i / 2))
      .runFold(0)(_ + _.sum))
  }

  @Benchmark
  def ZStreamChunk100() = {
    runZIO(ZStream.range(0, 1_000_000, 100)
      .mapZIO(i => ZIO.succeed(i * 4))
      .map(i => i / 2)
      .mapZIO(i => ZIO.succeed(i / 2))
      .runFold(0)(_ + _))
  }
}
