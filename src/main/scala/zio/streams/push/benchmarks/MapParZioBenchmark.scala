package zio.streams.push.benchmarks

import monix.eval.Task as MonixTask
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.openjdk.jmh.annotations.*
import zio.ZIO
import zio.stream.ZStream
import zio.streams.push.ChunkedPushStream.*
import zio.streams.push.{ChunkedPushStream, PushStream}

class MapParZioBenchmark extends BaseBenchmark {
  @Benchmark
  @OperationsPerInvocation(100_000)
  def observable() = {
    Observable.range(0, 100_000).mapParallelOrdered(4)(i => MonixTask.now(i * 2)).foldLeftL(0L)(_ + _).runSyncUnsafe()
  }

  @Benchmark
  @OperationsPerInvocation(100_000)
  def zStreamChunk1() = {
    runZIO(ZStream.range(0, 100_000, 1).mapZIOPar(4)(i => ZIO.succeed(i * 2)).runFold(0)(_ + _))
  }

  @Benchmark
  @OperationsPerInvocation(100_000)
  def zStreamChunk100() = {
    runZIO(ZStream.range(0, 100_000, 100).mapZIOPar(4)(i => ZIO.succeed(i * 2)).runFold(0)(_ + _))
  }

  @Benchmark
  @OperationsPerInvocation(100_000)
  def zStreamPlusRechunkChunk100() = {
    runZIO(ZStream.range(0, 100_000, 100).mapZIOPar(4)(i => ZIO.succeed(i * 2)).rechunk(100).runFold(0)(_ + _))
  }

  @Benchmark
  @OperationsPerInvocation(100_000)
  def pushStream() = {
    runZIO(PushStream.range(0, 100_000).mapZIOPar(4)(i => ZIO.succeed(i * 2)).runFold(0)(_ + _))
  }

  @Benchmark
  @OperationsPerInvocation(100_000)
  def pushStreamChunk100() = {
    runZIO(ChunkedPushStream.range(0, 100_000, 100).mapZIOParChunks(4)(i => ZIO.succeed(i * 2)).runFold(0)(_ + _.sum))
  }
}
