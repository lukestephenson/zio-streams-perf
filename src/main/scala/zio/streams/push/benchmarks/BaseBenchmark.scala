package zio.streams.push.benchmarks

import org.openjdk.jmh.annotations.*
import zio.ZIO

import java.util.concurrent.TimeUnit

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
abstract class BaseBenchmark {

  protected val StreamLength = 1_000_000

  private val zioRuntime = zio.Runtime.default

  protected def runZIO[A](io: zio.ZIO[Any, Throwable, A]): A = {
    zio.Unsafe.unsafe(implicit u => zioRuntime.unsafe.run(zio.ZIO.yieldNow.flatMap(_ => io)).getOrThrow())
  }

}
