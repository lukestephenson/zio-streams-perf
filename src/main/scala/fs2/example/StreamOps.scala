//package fs2.example
//
//import cats.effect.{Deferred, IO, Ref}
//import fs2.Stream
//
//object StreamOps {
//
//  private def repeatUntil[A](stream: Stream[IO, A])(predicate: A => Boolean): Stream[IO, A] = {
//    stream.flatMap(value => if (predicate(value)) Stream(value) ++ repeatUntil(stream)(predicate) else Stream(value))
//  }
//
//  def conflate[A, AGG](
//                        stream: Stream[IO, A],
//                        seed: A => AGG,
//                        aggregate: (AGG, A) => AGG): Stream[IO, AGG] = {
//
//    sealed trait State
//    case class Waiting(deferred: Deferred[IO, Unit]) extends State // waiting for data, plus signal to advise available
//    case class Aggregating(aggregated: Option[AGG]) extends State
//    case class Completed(finalAggregate: Option[AGG]) extends State
//
//    def aggElement(maybeAgg: Option[AGG], elem: A): Aggregating = {
//      val newAgg = maybeAgg match {
//        case Some(agg) => aggregate(agg, elem)
//        case None => seed(elem)
//      }
//      Aggregating(Some(newAgg))
//    }
//
//    Stream.eval(Ref.of[IO, State](Aggregating(None))).flatMap { stateRef =>
//
//      val aggregateElements = stream
//        .evalMap { elem =>
////          IO(println(s"consuming $elem")) >>
//          stateRef.modify {
//            case Aggregating(aggregated) => aggElement(aggregated, elem) -> IO.unit
//            case Waiting(deferred) => aggElement(None, elem) -> deferred.complete(())
//            case Completed(agg) => Completed(agg) -> IO.raiseError(new RuntimeException("Should not see elements once stream completes"))
//          }.flatten
//        }
//        .onComplete {
//          val complete: Stream[IO, Unit] = Stream.eval {
//            stateRef.modify {
//              case Aggregating(aggregated) => Completed(aggregated) -> IO.unit
//              case Waiting(deferred) => Completed(None) -> deferred.complete(()).void
//              case Completed(agg) =>
//                Completed(agg) -> IO.raiseError(new RuntimeException("Should not see complete when stream is already completed"))
//            }.flatten
//          }
//          complete
//        }
//
//      val dequeueLatestElement: Stream[IO, (Boolean, Option[AGG])] =
//        Stream.eval {
//          Deferred[IO, Unit].flatMap(deferred =>
//            stateRef.modify {
//              case Aggregating(maybeAgg) => Waiting(deferred) -> IO.pure(true -> maybeAgg)
//              case Completed(maybeAgg) => Completed(maybeAgg) -> IO.pure(false -> maybeAgg)
//              case Waiting(deferred) => Waiting(deferred) -> deferred.get.as(true -> None)
//            }.flatten
//          )
//        }
//
//      val terminatingStream = repeatUntil(dequeueLatestElement)(_._1).map(_._2).unNone
//
//      terminatingStream.concurrently(aggregateElements)
//    }
//  }
//}
//
//object implicits {
//  implicit class StreamExtensions[A](stream: Stream[IO, A]) {
//    def conflate[AGG](seed: A => AGG)(aggregate: (AGG, A) => AGG): Stream[IO, AGG] = {
//      StreamOps.conflate(stream, seed, aggregate)
//    }
//  }
//}
