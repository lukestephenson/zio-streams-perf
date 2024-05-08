package fs2.example

import zio.{ZIOAppDefault, *}
import zio.stream.{ZChannel, ZStream}
import zio.streams.push.PushStream

case class ExampleModel(value: Long)

object ZioExample extends ZIOAppDefault {
  val max = 10_000_000
  val chunkSize = 100_000

  implicit val trace: Trace = Trace.empty

  override def run = {
    val source: ZStream[Any, Nothing, Int] = ZStream.range(0, max, 1)

    val consumeWithFold: UIO[Long] = {
      source.runFold(0L) { case (elems, elem) => elems + elem }
    }

    val consumeWithRef: ZIO[Any, Nothing, Unit] = {
      for {
        ref <- Ref.make(List.empty[Long])
        _ <- source.mapZIO(elem => ref.update(elems => elem :: elems)).runDrain
        _ <- ref.get.flatMap(result => ZIO.succeed(println(s"number of elements ${result.size}")))
      } yield ()
    }

    val baselineListBuild = ZIO.succeed((1.toLong to max.toLong).foreach(i => ZChannel.write(ExampleModel(i)))).unit

//    val program = for {
////      _ <- timed("baselineListBuild", baselineListBuild)
//      _ <- timed("consumeWithFold", consumeWithFold.unit)
////      _ <- timed("consumeWithRef", consumeWithRef)
//      _ <- ZIO.sleep(10.seconds)
//    } yield ()
//
//    program.withRuntimeFlags(RuntimeFlags.disable(RuntimeFlag.CooperativeYielding))

    PushStream.range(1, 10)
      .tap(i => Console.printLine(s"got $i"))
      .buffer(5)
      .take(2)
      .tap(i => Console.printLine(s"take got $i").delay(1.second))
      .runCollect
  }

  private def timed(description: String, task: UIO[Unit]): ZIO[Any, Nothing, Unit] = {
    val iterations = 1
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
