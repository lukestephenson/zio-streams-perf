package zio.streams.push

import zio.streams.push.internal.Ack.Stop
import zio.streams.push.internal.{Observer, Observers, SourcePushStream}
import zio.{Chunk, URIO, ZIO}

object ChunkedPushStream {

  type ChunkedPushStream[R, E, A] = PushStream[R, E, Chunk[A]]

  def range(start: Int, end: Int, chunkSize: Int): ChunkedPushStream[Any, Nothing, Int] = {
    new SourcePushStream[Any, Nothing, Chunk[Int]] {
      override def startSource[OutR2 <: Any, OutE2 >: Nothing](observer: Observer[OutR2, OutE2, Chunk[Int]]): ZIO[OutR2, OutE2, Unit] = {
        loop(start, observer)
      }

      private def loop[R, E](next: Int, observer: Observer[R, E, Chunk[Int]]): URIO[R, Unit] = {
        when(next < end) {
          val max = Math.min(next + chunkSize, end)
          Observers.emitOne(observer, Chunk.fromArray(Array.range(next, max)), loop(max, observer))
        }
      }
    }
  }

  implicit class ChunkedPushStreamOps[R, E, A](stream: ChunkedPushStream[R, E, A]) {

    def mapChunks[A2](f: A => A2): ChunkedPushStream[R, E, A2] = {
      stream.map(chunkA => chunkA.map(f))
    }

    def mapZIOChunks[A2](f: A => ZIO[R, E, A2]): ChunkedPushStream[R, E, A2] = {
      stream.mapZIO(chunkA => chunkA.mapZIO(f))
    }

    def mapZIOParChunks[A2](parallelism: Int)(f: A => ZIO[R, E, A2]): ChunkedPushStream[R, E, A2] = {
      stream.mapZIOPar(parallelism)(chunkA => chunkA.mapZIO(f))
    }

    def mapConcatChunks[A2](f: A => Chunk[A2]): ChunkedPushStream[R, E, A2] = {
      stream.map(chunkA => chunkA.flatMap(a => f(a)))
    }
  }
}
