package zio.streams.push.internal.operators

import zio.ZIO
import zio.streams.push.PushStream
import zio.streams.push.PushStream.Operator
import zio.streams.push.internal.Observer

class LiftByOperatorPushStream[InR, InE, InA, OutR <: InR, OutE >: InE, OutB](
    upstream: PushStream[InR, InE, InA],
    operator: Operator[InA, OutR, OutE, OutB]) extends PushStream[OutR, OutE, OutB] {
  override def subscribe[OutR2 <: OutR, OutE2 >: OutE](observer: Observer[OutR2, OutE2, OutB]): ZIO[OutR2, OutE2, Unit] = {
    val transformedObserver: ZIO[OutR2, OutE2, Observer[OutR2, OutE2, InA]] = operator(observer)

    val subscribedObserver: ZIO[OutR2, OutE2, Unit] = transformedObserver
      .flatMap { subscriberB =>
        zio.Console.printLine(s"LiftByOperatorPushStream - subscribe").ignore *>
          upstream.subscribe(subscriberB)
            .onExit(exit => zio.Console.printLine(s"LiftByOperatorPushStream - $exit").ignore)
      }
    subscribedObserver
  }
}
