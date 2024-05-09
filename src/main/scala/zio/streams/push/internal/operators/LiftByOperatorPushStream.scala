package zio.streams.push.internal.operators

import zio.streams.push.PushStream
import zio.streams.push.PushStream.Operator
import zio.streams.push.internal.Observer
import zio.{RIO, URIO, ZIO}

class LiftByOperatorPushStream[InR, InE, InA, OutR <: InR, OutE >: InE, OutB](
    upstream: PushStream[InR, InE, InA],
    operator: Operator[InA, OutR, OutE, OutB]) extends PushStream[OutR, OutE, OutB] {
  override def subscribe[OutR2 <: OutR](observer: Observer[OutR2, OutE, OutB]): URIO[OutR2, Unit] = {
    val transformedObserver: URIO[OutR2, Observer[OutR2, OutE, InA]] = operator(observer)

    val subscribedObserver: URIO[OutR2, Unit] = transformedObserver
      .flatMap(subscriberB => upstream.subscribe(subscriberB))
    subscribedObserver
  }
}
