package zio.streams.push.demo

import zio.stream.ZStream
import zio.streams.push.ChunkedPushStream.*
import zio.streams.push.{ChunkedPushStream, PushStream}
import zio.{Chunk, Scope, ZIO, ZIOAppArgs, ZIOAppDefault}
import zio.*

import java.io.IOException

object PushSteamExample extends ZIOAppDefault {
  override def run: ZIO[Any with ZIOAppArgs with Scope, Any, Any] = {

    val range: PushStream[Any, Nothing, Int] = PushStream.range(0, 500_000)
    val mapped: PushStream[Any, String, Int] = range.mapZIO(i => if (i == 5) ZIO.fail("dead") else ZIO.succeed(i * 2))
    val folded: ZIO[Any, String, Long] = mapped.runFold(0L)(_ + _)

    val chunkSize = 100

    val streamMax = 5_000_000
    val pushStream = PushStream.range(0, streamMax)
    val zStream = ZStream.range(0, streamMax, 1)
    val zStreamChunks = ZStream.range(0, streamMax, chunkSize)

    val smallStreamSize = 300_000
    val smallZStream = ZStream.range(0, smallStreamSize, chunkSize)
    val smallPushStream = PushStream.range(0, smallStreamSize)

    val chunkedPushStream: PushStream[Any, Nothing, Chunk[Int]] =
      ChunkedPushStream.range(0, streamMax, chunkSize)

    val smallChunkedPushStream: ChunkedPushStream[Any, Nothing, Int] =
      ChunkedPushStream.range(0, smallStreamSize, chunkSize)

//      smallChunkedPushStream.map { }

    val program = for {
      _ <- ZIO.unit
      _ <- zio.Console.printLine(s"Running the tests with a chunk size of $chunkSize")
//      baseline = ZIO.succeed(Chunk.range(0, streamMax).flatMap { i => if (i % 2 == 0) Chunk.single(i) else Chunk.empty }.sum)
      baseline = ZIO.succeed {
        var sum: Long = 0
        (0 until streamMax).foreach(i =>
          if (i % 2 == 0) sum = sum + 1 else ()
        )
        sum
      }
//      rangeZS = zStream.runFold(0L)(_ + _)
//      rangePS = pushStream.runFold(0L)(_ + _)
//      rangeChunksPS = chunkedPushStream.runFold(0L)(_ + _.sum)
//      _ <- timed("baseline map * 2 and sum", baseline)
//      _ <- timed("baseline map * 2 and sum", baseline)
//      _ <- timed("baseline map * 2 and sum", baseline)
//      _ <- timed("ZS - range and fold", rangeZS)
//      _ <- timed("PS - range and fold", rangePS)
//      _ <- timed("PS - range and fold chunked", rangeChunksPS)
      mapZS = zStream.map(i => i * 2).runFold(0L)(_ + _)
      mapZSChunks = zStreamChunks.map(i => i * 2).runFold(0L)(_ + _)
      mapPS = pushStream.map(i => i * 2).runFold(0L)(_ + _)
      mapChunksPS = chunkedPushStream.mapChunks(i => i * 2).runFold(0L)(_ + _.sum)
      _ <- timed("ZS - map", mapZS)
      _ <- timed("PS - map", mapPS)
      _ <- timed("ZSChunks - map", mapZS)
//      _ <- timed("PS - mapChunks", mapChunksPS)
//      mapZioZS = zStream.mapZIO(i => ZIO.succeed(i * 2)).runFold(0L)(_ + _)
//      mapZioZSChunks = zStreamChunks.mapZIO(i => ZIO.succeed(i * 2)).runFold(0L)(_ + _)
//      mapZioPS = pushStream.mapZIO(i => ZIO.succeed(i * 2)).runFold(0L)(_ + _)
//      mapZioChunksPS = chunkedPushStream.mapZIOChunks(i => ZIO.succeed(i * 2)).runFold(0L)(_ + _.sum)
//      _ <- timed("ZS - mapZio", mapZioZS)
//      _ <- timed("PS - mapZio", mapZioPS)
//      _ <- timed("ZSChunks - mapZio", mapZioZSChunks)
//      _ <- timed("PSChunks - mapZio", mapZioChunksPS)
      mapZioParZS = smallZStream.mapZIOPar(8)(i => ZIO.succeed(i * 2)).runFold(0L)(_ + _)
      mapZioParPS = smallPushStream.mapZIOPar(8)(i => ZIO.succeed(i * 2)).runFold(0L)(_ + _)
      mapZioParChunksPS = smallChunkedPushStream.mapZIOParChunks(8)(i => ZIO.succeed(i * 2)).runFold(0L)(_ + _.sum)
//      _ <- timed("ZS - mapZioPar", mapZioParZS)
//      _ <- timed("PS - mapZioPar", mapZioParPS)
//      _ <- timed("PS - mapZioParChunks", mapZioParChunksPS)
//      concatMapZS = zStream.mapConcatChunk(i => if (i % 2 == 0) Chunk.single(i) else Chunk.empty).runFold(0L)(_ + _)
//      concatMapZSChunks = zStreamChunks.mapConcatChunk(i => if (i % 2 == 0) Chunk.single(i) else Chunk.empty).runFold(0L)(_ + _)
//      concatMapPS = pushStream.mapConcat(i => if (i % 2 == 0) Some(i) else None).runFold(0L)(_ + _)
//      concatMapChunksPS = chunkedPushStream.mapConcatChunks(i => if (i % 2 == 0) Chunk.single(i) else Chunk.empty).runFold(0L)(_ + _.sum)
//      _ <- timed("ZS - mapConcat", concatMapZS)
//      _ <- timed("PS - mapConcat", concatMapPS)
//      _ <- timed("ZSChunks - mapConcat", concatMapZSChunks)
//      _ <- timed("PSChunks - mapConcat", concatMapChunksPS)
    } yield ()

//    val program = PushStream.range(0, 10).mapZIO { i =>
//      if (i == 5) ZIO.fail("stream terminated") else ZIO.succeed(println(s"received $i")).as(i)
//    }.orElse(PushStream.range(11, 15)).runCollect.flatMap(r => ZIO.succeed(println(s"finished with $r")))
//
//    val program3 = ZStream.range(0, 10).mapZIO { i =>
//      if (i == 5) ZIO.fail("stream terminated") else ZIO.succeed(println(s"received $i")).as(i)
//    }.orElse(ZStream.range(11, 15)).runCollect.flatMap(r => ZIO.succeed(println(s"finished with $r")))

    val scheduled: PushStream[Any, Nothing, Int] = PushStream.range(10, 20).schedule(Schedule.fixed(1.second) && Schedule.recurs(5))
    val program2: ZIO[Any, IOException, Chunk[Unit]] = scheduled.mapZIO{i =>
      val x: IO[IOException, Unit] = zio.Console.printLine(s"saw $i")
      val y: IO[IOException, Unit] = x
      y
    }.runCollect

    val program3 = PushStream.range(1, 20).schedule(Schedule.fixed(1.second)).bufferSliding(1).mapZIO(i => zio.Console.printLine(s"got $i").ignore.delay(4500.milliseconds)).runDrain
    
    program3 *> zio.Console.printLine("done").ignore
//    PushStream.foo()
  }

  private def timed[R, E, A](description: String, task: ZIO[R, E, A]): ZIO[R, E, Unit] = {
    val iterations = 3
    val timedTask = for {
      result <- task.timed
      //      _ <- ZIO.sleep(1.second)
    } yield result

    val results = ZIO.foreach((1 to iterations).toList)(_ => timedTask)
    // 2 warm ups, then record
    task *> task *> results.flatMap { results =>
      val time = results.map(_._1.toMillis).sum / iterations
      val result = results.head._2
      ZIO.succeed(println(s"$description took ${time}ms on average over $iterations iterations to calculate $result"))
    }

  }
}
