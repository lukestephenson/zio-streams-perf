package fs2.example

import zio.stream.ZStream
import zio.{ZIOAppDefault, _}

object LukeExample extends ZIOAppDefault {
  val max = 10
  val chunkSize = 100_000

  override def run = {
    val source: LukeStream[Any, Nothing, Int] = LukeStream.range(0, max)

    val consumeWithFold: UIO[Long] =
      source.runFold(0L){case (elems, elem) => elem + elems }

    val program = for {
      _ <- timed("consumeWithFold", consumeWithFold.flatMap(result => ZIO.succeed(println(s"result : $result"))))
//      _ <- ZIO.sleep(10.seconds)
    } yield ()

    program.withRuntimeFlags(RuntimeFlags.disable(RuntimeFlag.CooperativeYielding))
  }

  private def timed(description: String, task: UIO[Unit]): ZIO[Any, Nothing, Unit] = {
    val iterations = 10
    val timedTask = for {
      result <- task.timed
      _ <- ZIO.sleep(1.second)
    } yield result

    val results: UIO[List[(zio.Duration, Unit)]] = ZIO.foreach((1 to iterations).toList)(_ => timedTask)
    results.map(_.map(_._1.toMillis).sum / iterations).flatMap(time =>
      ZIO.succeed(println(s"$description of $max elements took ${time}ms on average over $iterations iterations"))
    )
  }

//  val chunkedSource = (0 to (max / chunkSize)).foldLeft[Stream[IO, Int]](Stream.empty) { case (agg, index) =>
//    val base = index * chunkSize
//    val chunk = Chunk.seq(base to (base + chunkSize -1))
//    agg ++ Stream.chunk(chunk)
//  }

}
