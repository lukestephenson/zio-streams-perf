package fs2.example

import zio.{ZIOAppDefault, _}
import zio.stream.{ZChannel, ZStream}
import zio.streams.push.PushStream

object ZioFoldExample extends ZIOAppDefault {
//  val max = 1_000_000

  override def run = {

    val range: ZStream[Any, Nothing, Int] = ZStream.range(0, 500_000)
    val mapped: ZStream[Any, String, Int] = range.mapZIO(i => if (i == 5) ZIO.fail("dead") else ZIO.succeed(i * 2))
    val folded: ZIO[Any, String, Long] = mapped.runFold(0L)(_ + _)

    def runWithSize(max: Int) = {
      val source: ZStream[Any, Nothing, Int] = ZStream.range(min = 0, max = max, chunkSize = 1)
        .mapZIOPar(8)(i => ZIO.succeed(i * 2))
//        .mapZIO(i => ZIO.succeed(i*2))

      val consumeWithFold: UIO[Long] = {
        source.runFold(0L) { case (sum, elem) => sum + elem }
      }

      source ++ source

      timed(s"consumeWithFold of $max elements", consumeWithFold)
    }

    val program = for {
//      _ <- timed("foreach", ZIO.foreachDiscard(1 to 50_000_000)(i => ZIO.succeed(i * 2)))
//      _ <- runWithSize(100_000)
//      _ <- runWithSize(200_000)
//      _ <- runWithSize(300_000)
//      _ <- runWithSize(400_000)
      _ <- runWithSize(500_000)
//      _ <- ZIO.sleep(10.seconds)
    } yield ()

    program // .withRuntimeFlags(RuntimeFlags.disable(RuntimeFlag.CooperativeYielding))
  }

  private def timed(description: String, task: UIO[_]): ZIO[Any, Nothing, Unit] = {
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

//  val chunkedSource = (0 to (max / chunkSize)).foldLeft[Stream[IO, Int]](Stream.empty) { case (agg, index) =>
//    val base = index * chunkSize
//    val chunk = Chunk.seq(base to (base + chunkSize -1))
//    agg ++ Stream.chunk(chunk)
//  }

}
