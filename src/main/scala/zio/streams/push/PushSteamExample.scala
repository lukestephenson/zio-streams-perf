package zio.streams.push

import zio.stream.ZStream
import zio.{Chunk, Scope, ZIO, ZIOAppArgs, ZIOAppDefault}
import ChunkedPushStream.*

object PushSteamExample extends ZIOAppDefault {
  override def run: ZIO[Any with ZIOAppArgs with Scope, Any, Any] = {

    val range: PushStream[Any, Nothing, Int] = PushStream.range(0, 500_000)
    val mapped: PushStream[Any, String, Int] = range.mapZIO(i => if (i == 5) ZIO.fail("dead") else ZIO.succeed(i * 2))
    val folded: ZIO[Any, String, Long] = mapped.runFold(0L)(_ + _)

    val chunkSize = 100

    val streamMax = 5_000_000
    val pushStream = PushStream.range(0, streamMax)
    val zStream = ZStream.range(0, streamMax, chunkSize)

    val smallStreamSize = 300_000
    val smallZStream = ZStream.range(0, smallStreamSize, chunkSize)
    val smallPushStream = PushStream.range(0, smallStreamSize)

    val chunkedPushStream: PushStream[Any, Nothing, Chunk[Int]] =
      PushStream.fromIterable((0 until streamMax / chunkSize).map(i => Chunk.range(i * chunkSize, i * chunkSize + chunkSize)))

    val smallChunkedPushStream: PushStream[Any, Nothing, Chunk[Int]] =
      PushStream.fromIterable((0 until smallStreamSize / chunkSize).map(i => Chunk.range(i * chunkSize, i * chunkSize + chunkSize)))

    val program = for {
      _ <- ZIO.unit
      rangeZS = zStream.runFold(0L)(_ + _)
      rangePS = pushStream.runFold(0L)(_ + _)
      rangeChunksPS = chunkedPushStream.runFold(0L)(_ + _.sum)
      _ <- timed("ZS - range and fold", rangeZS)
      _ <- timed("PS - range and fold", rangePS)
      _ <- timed("PS - range and fold chunked", rangeChunksPS)
      mapZS = zStream.map(i => i * 2).runFold(0L)(_ + _)
      mapPS = pushStream.map(i => i * 2).runFold(0L)(_ + _)
      mapChunksPS = chunkedPushStream.mapChunks(i => i * 2).runFold(0L)(_ + _.sum)
      _ <- timed("ZS - map", mapZS)
      _ <- timed("PS - map", mapPS)
      _ <- timed("PS - mapChunks", mapChunksPS)
      mapZioZS = zStream.mapZIO(i => ZIO.succeed(i * 2)).runFold(0L)(_ + _)
      mapZioPS = pushStream.mapZIO(i => ZIO.succeed(i * 2)).runFold(0L)(_ + _)
      mapZioChunksPS = chunkedPushStream.mapZIOChunks(i => ZIO.succeed(i * 2)).runFold(0L)(_ + _.sum)
      _ <- timed("ZS - mapZio", mapZioZS)
      _ <- timed("PS - mapZio", mapZioPS)
      _ <- timed("PS - mapZioChunks", mapZioChunksPS)
      mapZioParZS = smallZStream.mapZIOPar(8)(i => ZIO.succeed(i * 2)).runFold(0L)(_ + _)
      mapZioParPS = smallPushStream.mapZIOPar(8)(i => ZIO.succeed(i * 2)).runFold(0L)(_ + _)
      mapZioParChunksPS = smallChunkedPushStream.mapZIOParChunks(8)(i => ZIO.succeed(i * 2)).runFold(0L)(_ + _.sum)
      _ <- timed("ZS - mapZioPar", mapZioParZS)
      _ <- timed("PS - mapZioPar", mapZioParPS)
      _ <- timed("PS - mapZioParChunks", mapZioParChunksPS)
    } yield ()

    program
  }

  private def timed[R, E, A](description: String, task: ZIO[R, E, A]): ZIO[R, E, Unit] = {
    val iterations = 5
    val timedTask = for {
      result <- task.timed
      //      _ <- ZIO.sleep(1.second)
    } yield result

    val results = ZIO.foreach((1 to iterations).toList)(_ => timedTask)
    results.flatMap { results =>
      val time = results.map(_._1.toMillis).sum / iterations
      val result = results.head._2
      ZIO.succeed(println(s"$description took ${time}ms on average over $iterations iterations to calculate $result"))
    }
  }
}