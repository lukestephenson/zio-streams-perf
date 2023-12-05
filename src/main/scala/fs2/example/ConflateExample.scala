//package fs2.example
//
//import cats.data.NonEmptyList
//import cats.effect.{IO, IOApp}
//import fs2.Stream
//import fs2.example.implicits.StreamExtensions
//
//import scala.concurrent.duration.DurationInt
//
//object ConflateExample extends IOApp.Simple {
//  override def run: IO[Unit] = {
//    val source = Stream.range[IO, Int](0, 10, 1).meteredStartImmediately(1000.millis)
//
//    val now = IO(System.currentTimeMillis())
//
//    def observable(startTime: Long): IO[Unit] = {
//      source
//        .conflate(NonEmptyList.one)((elems, elem) => elem :: elems).meteredStartImmediately(2600.millis)
//        .evalMap { elements => now.flatMap(now => IO(println(s"got batch of ${elements} at ${now - startTime}ms"))) }
//        .compile.drain
//    }
//
//    for {
//      startTime <- now
//      _ <- observable(startTime)
//      endTime <- now
//      _ <- IO(println(s"Took ${endTime - startTime}ms"))
//    } yield ()
//  }
//}
