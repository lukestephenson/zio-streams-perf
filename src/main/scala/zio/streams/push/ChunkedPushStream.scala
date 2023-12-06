package zio.streams.push

import zio.{Chunk, ZIO}

object ChunkedPushStream {
  extension[R, E, A](stream: PushStream[R, E, Chunk[A]]) {
    def mapChunks[A2](f: A => A2): PushStream[R, E, Chunk[A2]] = {
      stream.map(chunkA => chunkA.map(f))
    }

    def mapZIOChunks[A2](f: A => ZIO[R, E, A2]): PushStream[R, E, Chunk[A2]] = {
      stream.mapZIO(chunkA => chunkA.mapZIO(f))
    }

    def mapZIOParChunks[A2](parallelism: Int)(f: A => ZIO[R, E, A2]): PushStream[R, E, Chunk[A2]] = {
      stream.mapZIOPar(parallelism)(chunkA => chunkA.mapZIO(f))
    }
  }
}
