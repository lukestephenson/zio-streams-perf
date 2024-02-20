package zio.streams.push.internal

import zio.{URIO, ZIO}

object Observers {
  def emitAll[R, E, A](observer: Observer[R, E, A], iterable: Iterable[A]): URIO[R, Ack] = {
    emitAll(observer, iterable.iterator)
  }

  def emitAll[R, E, A](observer: Observer[R, E, A], iterator: Iterator[A]): URIO[R, Ack] = {
    def loop(): URIO[R, Ack] = {
      if (iterator.hasNext) {
        emit(observer, iterator.next(), loop())
      } else {
        Acks.Continue
      }
    }

    loop()
  }

  inline def emit[R, E, A](observer: Observer[R, E, A], element: A, continue: => URIO[R, Ack]): URIO[R, Ack] = {
    observer.onNext(element).flatMap {
      case Ack.Stop => Acks.Stop
      case Ack.Continue => continue
    }
  }

  inline def emitOne[R, E, A](observer: Observer[R, E, A], element: A, continue: => URIO[R, Unit]): URIO[R, Unit] = {
    observer.onNext(element).flatMap {
      case Ack.Stop => ZIO.unit
      case Ack.Continue => continue
    }
  }
}
