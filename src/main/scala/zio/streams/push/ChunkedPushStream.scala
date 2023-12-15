package zio.streams.push

import zio.streams.push.Ack.Stop
import zio.streams.push.Acks.Continue
import zio.{Chunk, ZIO}

object ChunkedPushStream {

  def range(start: Int, end: Int, chunkSize: Int) = {
    new PushStream[Any, Nothing, Chunk[Int]] {
      override def subscribe[OutR2 <: Any, OutE2 >: Nothing](observer: Observer[OutR2, OutE2, Chunk[Int]]): ZIO[OutR2, OutE2, Unit] = {
        loop(start, observer)
      }

      private def loop[R, E](next: Int, observer: Observer[R, E, Chunk[Int]]): ZIO[R, E, Unit] = {
        if (next >= end) observer.onComplete()
        else {
          val max = next + chunkSize
          Observers.emitOne(observer, Chunk.fromArray(Array.range(next, max)), loop(max, observer))
        }
      }
    }
  }

  extension[R, E, A](stream: PushStream[R, E, Chunk[A]]) {

    def mapChunks[A2](f: A => A2): PushStream[R, E, Chunk[A2]] = {
      stream.map(chunkA => chunkA.map(f))
    }

    def mapZIOChunks[A2](f: A => ZIO[R, E, A2]): PushStream[R, E, Chunk[A2]] = {
      stream.mapZIO(chunkA => chunkA.mapZIO(f))
    }

    def mapZIOParChunks[A2](parallelism: Int)(f: A => ZIO[R, E, A2]): PushStream[R, E, Chunk[A2]] = {
      stream.mapZIOPar(parallelism)(chunkA => chunkA.mapZIO(f))
    }

    def mapConcatChunks[A2](f: A => Chunk[A2]): PushStream[R, E, Chunk[A2]] = {
      stream.map(chunkA => chunkA.flatMap(a => f(a)))
    }
  }
}
