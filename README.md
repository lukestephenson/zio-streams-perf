An investigation into a push stream based ZIO streaming implementation

History

Refer to https://gist.github.com/lukestephenson/9ee6639f8a63d30e8573571f05d6498f

TLDR; This is motivated by the desire to have streaming applications in ZIO that perform just as well as they did with Monix Observable. It could have started with with cats-effect or ZIO, but I prefer ZIO having an explicit error channel (the environment I'm still undecided about and isn't a motivating factor).

As the gist above mentions, I believe the primary reason for the difference in performance between Monix Observable and ZIO Streams is the push vs pull based implementation. Can a push based streaming implementation on ZIO be as fast as Monix Observable?

This experiment attempts to introduce a ZIO native push based streaming implementation as a comparison point for performance. It is heavily inspired by the implementation of Monix Observable, but with some key differences.

# Key design motivations

## Based on Monix Observable
Monix Observable has:
```
trait Observer[-T] {
  def onNext(elem: T): Future[Ack]
```

While PushStream implementation has:
```
trait Observer[R, +E, -T] {
  def onNext(elem: T): ZIO[R, E, Ack]
```

The Monix implementation is based on Future (as opposed to Monix Task). This means it needs to deal with a lot of things that Task gives you for free (cancellation, cooperative work scheduling). Things like the [RangeObservable](https://github.com/monix/monix/blob/2faa2cf7425ab0b88ea57b1ea193bce16613f42a/monix-reactive/shared/src/main/scala/monix/reactive/internal/builders/RangeObservable.scala#L59) need to deal with the complexities of when to yield to other operations.

By basing on ZIO (and not Future), I'm hopeful that the implementation of individual PushStream operations is greatly simplified.

## Chunking is not a hidden concern

Chunking will always make any streaming implementation faster, be it push or pull based. Introducing the complexities of streaming is always going to make it slower to do a `SomeReactiveStream.map` than a `List.map`. That said, it shouldn't be a workaround for slow performance.

So while I am supportive of Streams using Chunking, I'm not of it being a hidden concern that can break performance.

ZStream.mapZIOPar has the following note in the documentation:
> Note: This combinator destroys the chunking structure. It's recommended to use rechunk

I really don't like this. Every operation on the Stream should be efficient. If it is not, this should be explicitly communicated through the API / types.

I've instead opted to model this as a `PushStream[R,E,Chunk[A]]`. It is up to the user to decide what data structure to user that provides batches. Greatest efficiency with both still requires the stream to start with Chunks.

With the `Chunk[A]` explicitly modelled, this means that operations on the `PushStream` like `map` make it incredibly clear that action is occurring on a chunk.

However, this doesn't prevent convenience methods for working with chunks being expressed by an extension method `extension[R, E, A](stream: PushStream[R, E, Chunk[A]])` and we can even have a friendly type alias `type ChunkedPushStream[R, E, A] = PushStream[R, E, Chunk[A]]`. We now have the best of both worlds. A relatively efficient streaming implementation. Where users of the stream can further benefit from chunking. But there shouldn't be any operations which break chunking or performance without the user being explicitly aware of that (eg calling a method called rechunk).

# Benchmarks

The last benchmark result has been committed to this project. You can view the results thanks to https://jmh.morethan.io/.

- [Benchmark visualisation](https://jmh.morethan.io/?sources=)

## Next steps

I acknowledge this push based streaming implementation is far from perfect. But hopefully it is complete enough to serve as a comparison for discussion (if not I can try to improve).

I'm keen to understand why pull based streams are so much more popular than pushed based streams. If pushed based streams are faster due to their implementation, then I'm interested to see if there is demand for a push based streaming implementation.