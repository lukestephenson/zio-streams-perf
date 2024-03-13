package zio.interop

import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import zio.{Chunk, Scope, Task, Trace, UIO, ZIO}
import zio.stream.ZSink
import zio.stream.ZStream
import zio.streams.push.PushStream

package object reactivestreams {

  final implicit class publisherToPushStream[O](private val publisher: Publisher[O]) extends AnyVal {

    /** Create a `Stream` from a `Publisher`.
     * @param qSize
     *   The size used as internal buffer. If possible, set to a power of 2 value for best performance.
     */
    def toZIOStream(qSize: Int = 16)(implicit trace: Trace): ZStream[Any, Throwable, O] =
      Adapters.publisherToStream(publisher, qSize)

    def toPushStream(qSize: Int = 16)(implicit trace: Trace): PushStream[Any, Throwable, Chunk[O]] =
      Adapters.publisherToPushStream(publisher, qSize)
  }

}
