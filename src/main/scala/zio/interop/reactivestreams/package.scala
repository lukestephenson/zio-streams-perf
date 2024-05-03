package zio.interop

import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import zio.{ Scope, UIO, Task, ZIO, Trace }
import zio.stream.ZSink
import zio.stream.ZStream

package object reactivestreams {

  final implicit class publisherToPushStream[O](private val publisher: Publisher[O]) extends AnyVal {

    /** Create a `Stream` from a `Publisher`.
     * @param qSize
     *   The size used as internal buffer. If possible, set to a power of 2 value for best performance.
     */
    def toZIOStream(qSize: Int = 16)(implicit trace: Trace): ZStream[Any, Throwable, O] =
      Adapters.publisherToPushStream(publisher, qSize)
  }

}
