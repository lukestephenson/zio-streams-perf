package zio.streams.push.internal

import zio.ZIO

object Observers {
  def emitAll[R, E, A](observer: Observer[R, E, A], iterable: Iterable[A]): ZIO[R, E, Ack] = {
    emitAll(observer, iterable.iterator)
  }

  def emitAll[R, E, A](observer: Observer[R, E, A], iterator: Iterator[A]): ZIO[R, E, Ack] = {
    def loop(): ZIO[R, E, Ack] = {
      if (iterator.hasNext) {
        emit(observer, iterator.next(), loop())
      } else {
        Acks.Continue
      }
    }

    loop()
  }

  inline def emit[R, E, A](observer: Observer[R, E, A], element: A, continue: => ZIO[R, E, Ack]): ZIO[R, E, Ack] = {
    observer.onNext(element).flatMap {
      case Ack.Stop => Acks.Stop
      case Ack.Continue => continue
    }
  }

  inline def emitOne[R, E, A](observer: Observer[R, E, A], element: A, continue: => ZIO[R, E, Unit]): ZIO[R, E, Unit] = {
    observer.onNext(element).flatMap {
      case Ack.Stop => ZIO.unit
      case Ack.Continue => continue
    }
  }
}
