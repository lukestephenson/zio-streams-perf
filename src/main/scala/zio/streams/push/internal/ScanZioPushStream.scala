package zio.streams.push.internal

import zio.streams.push.PushStream
import zio.streams.push.internal.operators.DefaultObserver
import zio.{URIO, ZIO}

class ScanZioPushStream[InR, InE, InA, OutR <: InR, OutE >: InE, S](
    upstream: PushStream[InR, InE, InA],
    seed: S,
    op: (S, InA) => ZIO[OutR, OutE, S]) extends PushStream[OutR, OutE, S] {

  override def subscribe[OutR2 <: OutR, OutE2 >: OutE](observer: Observer[OutR2, OutE2, S]): URIO[OutR2, Unit] = {
    val subscription = upstream.subscribe(new DefaultObserver[OutR2, OutE2, InA](observer) {
      var state = seed
      override def onNext(elem: InA): URIO[OutR2, Ack] = {
        op(state, elem).foldZIO(
          e => observer.onError(e).as(Ack.Stop),
          newState => {
            state = newState
            observer.onNext(newState)
          }
        )
      }
    })

    // Start by emitting the seed value, then continue with the subscription
    Observers.emitOne(observer, seed, subscription)
  }
}
