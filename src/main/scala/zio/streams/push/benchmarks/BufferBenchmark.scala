package zio.streams.push.benchmarks

import monix.eval.Task as MonixTask
import monix.execution.Scheduler.Implicits.global
import monix.reactive.{Observable, OverflowStrategy}
import org.openjdk.jmh.annotations.*
import zio.stream.ZStream
import zio.streams.push.{ChunkedPushStream, PushStream}

class BufferBenchmark extends BaseBenchmark {
  @Benchmark
  def observable() = {
    Observable.range(0, 1_000_000)
      .asyncBoundary(OverflowStrategy.BackPressure(8))
      .foldLeftL(0L)(_ + _).runSyncUnsafe()
  }

  @Benchmark
  def pushStream() = {
    runZIO(PushStream.range(0, 1_000_000)
      .buffer(8)
      .runFold(0)(_ + _))
  }

  @Benchmark
  def zStream() = {
    runZIO(
      ZStream.range(0, 1_000_000, 1)
        .buffer(8)
        .runFold(0)(_ + _)
    )
  }

//  @Benchmark
//  def kyoStreamBuffer() = {
//    val seq = 0 to 1_000_000
//    KyoApp.run(Streams.initSeq(seq).buffer(8).runFold(0)(_ + _))
//  }

}
