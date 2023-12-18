package zio.streams.push

import org.openjdk.jmh.annotations.*
import zio.ZIO
import zio.stream.ZStream

import java.util.concurrent.TimeUnit
import ChunkedPushStream.*

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 3, timeUnit = TimeUnit.SECONDS, time = 3)
@Warmup(iterations = 3, timeUnit = TimeUnit.SECONDS, time = 3)
@Fork(
  value = 3,
  jvmArgsAppend = Array("-Dcats.effect.tracing.mode=none", "-Dcats.effect.auto.yield.threshold.multiplier=20")
) // zio uses a variable threshold, but roughly 10,240
@Threads(value = 1)
@OperationsPerInvocation(1_000_000)
class Benchmarks {

  private val zioRuntime = zio.Runtime.default

  private[this] def runZIO[A](io: zio.ZIO[Any, Throwable, A]): A =
    zio.Unsafe.unsafe(implicit u => zioRuntime.unsafe.run(zio.ZIO.yieldNow.flatMap(_ => io)).getOrThrow())

//  @Benchmark
//  def zStreamFoldChunk1() = {
//    runZIO(ZStream.range(0, 1_000_000, 1).runFold(0)(_+_))
//  }
//
//  @Benchmark
//  def zStreamFoldChunk100() = {
//    runZIO(ZStream.range(0, 1_000_000, 100).runFold(0)(_ + _))
//  }
//
//  @Benchmark
//  def pStreamFold() = {
//    runZIO(PushStream.range(0, 1_000_000).runFold(0)(_ + _))
//  }
//
//  @Benchmark
//  def pStreamFoldChunk100() = {
//    runZIO(ChunkedPushStream.range(0, 1_000_000, 100).runFold(0)(_ + _.sum))
//  }
//
//  @Benchmark
//  def zStreamMapChunk1() = {
//    runZIO(ZStream.range(0, 1_000_000, 1).map(_*2).runFold(0)(_ + _))
//  }
//
//  @Benchmark
//  def zStreamMapChunk100() = {
//    runZIO(ZStream.range(0, 1_000_000, 100).map(_*2).runFold(0)(_ + _))
//  }
//
//  @Benchmark
//  def pStreamMap() = {
//    runZIO(PushStream.range(0, 1_000_000).map(_ *2).runFold(0)(_ + _))
//  }
//
//  @Benchmark
//  def pStreamMapChunk100() = {
//    runZIO(ChunkedPushStream.range(0, 1_000_000, 100).mapChunks(_ *2).runFold(0)(_ + _.sum))
//  }
//
//  @Benchmark
//  def zStreamMapZioChunk1() = {
//    runZIO(ZStream.range(0, 1_000_000, 1).mapZIO(i => ZIO.succeed(i * 2)).runFold(0)(_ + _))
//  }
//
//  @Benchmark
//  def zStreamMapZioChunk100() = {
//    runZIO(ZStream.range(0, 1_000_000, 100).mapZIO(i => ZIO.succeed(i * 2)).runFold(0)(_ + _))
//  }
//
//  @Benchmark
//  def pStreamMapZio() = {
//    runZIO(PushStream.range(0, 1_000_000).mapZIO(i => ZIO.succeed(i * 2)).runFold(0)(_ + _))
//  }
//
//  @Benchmark
//  def pStreamMapZioChunk100() = {
//    runZIO(ChunkedPushStream.range(0, 1_000_000, 100).mapZIOChunks(i => ZIO.succeed(i * 2)).runFold(0)(_ + _.sum))
//  }

  @Benchmark
  @OperationsPerInvocation(100_000)
  def zStreamMapZioParChunk1() = {
    runZIO(ZStream.range(0, 100_000, 1).mapZIOPar(4)(i => ZIO.succeed(i * 2)).runFold(0)(_ + _))
  }

  @Benchmark
  @OperationsPerInvocation(100_000)
  def zStreamMapZioParChunk100() = {
    runZIO(ZStream.range(0, 100_000, 100).mapZIOPar(4)(i => ZIO.succeed(i * 2)).runFold(0)(_ + _))
  }

  @Benchmark
  @OperationsPerInvocation(100_000)
  def zStreamMapZioParPlusRechunkChunk100() = {
    runZIO(ZStream.range(0, 100_000, 100).mapZIOPar(4)(i => ZIO.succeed(i * 2)).rechunk(100).runFold(0)(_ + _))
  }

  @Benchmark
  @OperationsPerInvocation(100_000)
  def pStreamMapParZio() = {
    runZIO(PushStream.range(0, 100_000).mapZIOPar(4)(i => ZIO.succeed(i * 2)).runFold(0)(_ + _))
  }

  @Benchmark
  @OperationsPerInvocation(100_000)
  def pStreamMapZioParChunk100() = {
    runZIO(ChunkedPushStream.range(0, 100_000, 100).mapZIOParChunks(4)(i => ZIO.succeed(i * 2)).runFold(0)(_ + _.sum))
  }
}
