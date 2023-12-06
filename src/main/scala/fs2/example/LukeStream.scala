package fs2.example

import zio._

class LukeStream[-R, +E, +A](val ops: LukeOps[A]) {
  def runFold[B](z: B)(fn: (B, A) => B): UIO[B] = {
    def go(agg: B): LukeOps[B] = {
      LukeOps.Read[A, B](element => LukeOps.Write(fn(agg, element)))
    }

    val channel: LukeOps[B] = go(z)

//    Runner.run(channel)
    ???
  }

  // def runFold[B](z: B)(fn: (B, A) => B): UIO[B] = {
  //    def run(o: LukeOps[A], agg: B): B = {
  //      o match {
  //        case LukeOps.Suspend(ops) => run(ops(), agg)
  //        case LukeOps.Write(e) => fn(agg, e)
  //        case LukeOps.Fold(self, done) => {
  //          val selfResult: B = run(self, agg)
  //
  //          run(done(), selfResult)
  //        }
  //      }
  //    }
  //
  //    ZIO.succeed(run(ops, z))
  //  }
}

object LukeStream {
  def range(start: Int, end: Int): LukeStream[Any, Nothing, Int] = {
    def go(min: Int, max: Int): LukeOps[Int] = {
      if (min == max) LukeOps.Write(min)
      else LukeOps.Write(min) ++ go(min + 1, max)
    }
    LukeStream.suspend {
      new LukeStream(go(start, end))
    }
  }

  def suspend[R, E, A](stream: => LukeStream[R, E, A]) = {
    new LukeStream(LukeOps.Suspend(() => stream.ops))
  }
}

sealed trait LukeOps[+OutElem] { self =>
  def ++[OutElem1 >: OutElem](next: => LukeOps[OutElem1]): LukeOps[OutElem1] = {
    LukeOps.Fold(self, () => next)
  }
}

object LukeOps {
  case class Suspend[OutElem](ops: () => LukeOps[OutElem]) extends LukeOps[OutElem]
  case class Write[OutElem](e: OutElem) extends LukeOps[OutElem]
  case class Fold[OutElem](self: LukeOps[OutElem], done: () => LukeOps[OutElem]) extends LukeOps[OutElem]
  case class Read[InElem, OutElem](fn: InElem => LukeOps[OutElem]) extends LukeOps[OutElem]
}

object Runner {
//  val readers: Stack[LukeOps.Read]

  def run[OutElement](lukeOps: LukeOps[OutElement]): OutElement = {
    lukeOps match {
      case LukeOps.Suspend(ops) => run(ops())
      case LukeOps.Write(e) => ???
      case LukeOps.Fold(self, done) => ???
      case LukeOps.Read(fn) => ???
    }
  }
}
