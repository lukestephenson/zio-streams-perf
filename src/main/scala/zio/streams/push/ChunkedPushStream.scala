package zio.streams.push

import zio.streams.push.PushStream.repeatZIOOption
import zio.streams.push.internal.Ack.Stop
import zio.streams.push.internal.{Observer, Observers, ScanZioChunkedPushStream, SourcePushStream}
import zio.{Chunk, Dequeue, Trace, URIO, ZIO}

type ChunkedPushStream[R, E, A] = PushStream[R, E, Chunk[A]]

object ChunkedPushStream {

  /** The default chunk size used by the various combinators and constructors of [[ZStream]].
    */
  final val DefaultChunkSize = 4096

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

  /** Creates a stream from a queue of values. The queue will be shutdown once the stream is closed.
    *
    * @param maxChunkSize
    *   Maximum number of queued elements to put in one chunk in the stream
    */
  def fromQueueWithShutdown[O](
      queue: => Dequeue[O],
      maxChunkSize: => Int = DefaultChunkSize)(implicit trace: Trace): ChunkedPushStream[Any, Nothing, O] =
    fromQueue(queue, maxChunkSize) // TODO add queue shutdown .ensuring(queue.shutdown)

  /** Creates a stream from a queue of values
    *
    * @param maxChunkSize
    *   Maximum number of queued elements to put in one chunk in the stream
    */
  def fromQueue[O](
      queue: => Dequeue[O],
      maxChunkSize: => Int = DefaultChunkSize)(implicit trace: Trace): ChunkedPushStream[Any, Nothing, O] =
    repeatZIOOption {
      queue
        .takeBetween(1, maxChunkSize)
        .catchAllCause(c =>
          queue.isShutdown.flatMap { down =>
            if (down && c.isInterrupted) ZIO.fail(None) // Pull.end
            else ??? // TODO Pull.failCause(c)
          }
        )
    }

  extension [R, E, A](stream: ChunkedPushStream[R, E, A]) {

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

    def mapConcatChunksIterable[A2](f: A => Iterable[A2]): ChunkedPushStream[R, E, A2] = {
      stream.map(iterableA => Chunk.fromIterable(iterableA.flatMap(a => f(a))))
    }

    def scanZIOChunks[S](s: => S)(f: (S, A) => ZIO[R, E, S]): ChunkedPushStream[R, E, S] = {
      new ScanZioChunkedPushStream(stream, s, f)
    }
  }
}
