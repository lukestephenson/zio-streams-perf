package zio.streams.push.internal

import zio.streams.push.ChunkedPushStream
import zio.streams.push.internal.operators.DefaultObserver
import zio.{Chunk, URIO, ZIO}

// Largely a copy paste of ScanZioPushStream. Opportunity for reuse
class ScanZioChunkedPushStream[InR, InE, InA, OutR <: InR, OutE >: InE, S](
    upstream: ChunkedPushStream[InR, InE, InA],
    seed: S,
    op: (S, InA) => ZIO[OutR, OutE, S]) extends ChunkedPushStream[OutR, OutE, S] {

  override def subscribe[OutR2 <: OutR, OutE2 >: OutE](observer: Observer[OutR2, OutE2, Chunk[S]]): URIO[OutR2, Unit] = {
    val subscription = upstream.subscribe(new DefaultObserver[OutR2, OutE2, Chunk[InA]](observer) {
      var state = seed
      override def onNext(elem: Chunk[InA]): URIO[OutR2, Ack] = {
        def mapAccumChunks(localSeed: S): ZIO[OutR2, OutE2, (S, Chunk[S])] = {
          elem.mapAccumZIO(localSeed)((s1, a) => op(s1, a).map(b => (b, b)))
        }
        mapAccumChunks(state).foldZIO(
          e => observer.onError(e).as(Ack.Stop),
          newState => {
            state = newState._1
            observer.onNext(newState._2)
          }
        )
      }
    })

    // Start by emitting the seed value, then continue with the subscription
    Observers.emitOne(observer, Chunk.single(seed), subscription)
  }
}
