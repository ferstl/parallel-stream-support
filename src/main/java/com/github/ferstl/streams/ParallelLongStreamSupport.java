package com.github.ferstl.streams;

import java.util.Arrays;
import java.util.LongSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.PrimitiveIterator.OfLong;
import java.util.Spliterator;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.function.BiConsumer;
import java.util.function.LongBinaryOperator;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.LongPredicate;
import java.util.function.LongSupplier;
import java.util.function.LongToDoubleFunction;
import java.util.function.LongToIntFunction;
import java.util.function.LongUnaryOperator;
import java.util.function.ObjLongConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.stream.StreamSupport.longStream;

/**
 * <p>
 * An implementation of {@link LongStream} which uses a custom {@link ForkJoinPool} for parallel aggregate operations.
 * This is the {@code long} primitive specialization of {@link ParallelStreamSupport}.
 * <p>
 * The following example illustrates an aggregate operation using {@link ParallelStreamSupport} and
 * {@link ParallelLongStreamSupport} with a custom {@link ForkJoinPool}, computing the sum of the weights of the red
 * widgets:
 *
 * <pre>
 *
 * ForkJoinPool pool = new ForkJoinPool();
 * long sum = ParallelStreamSupport.parallelStream(widgets, pool)
 *     .filter(w -&gt; w.getColor() == RED)
 *     .mapToLong(w -&gt; w.getWeight())
 *     .sum();
 * </pre>
 * <p>
 * In case this stream is configured for parallel execution, i.e. {@link #isParallel()} returns {@code true}, a
 * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
 * operation</a> will be executed as {@link ForkJoinTask} in the custom {@link ForkJoinPool}. Otherwise it will be
 * executed in the calling thread.
 * </p>
 * This implementation offers various factory methods which are based on:
 * <ul>
 * <li>The static factory methods of {@link LongStream}, which are meaningful for parallel streams</li>
 * <li>{@link Arrays#stream(long[])}</li>
 * <li>{@link StreamSupport#longStream(Spliterator.OfLong, boolean)}</li>
 * <li>{@link StreamSupport#longStream(Supplier, int, boolean)}</li>
 * </ul>
 *
 * @apiNote
 * <p>
 * Internally, this stream wraps a {@code long} stream which is initially created in one of the static factory methods.
 * Whenever a non-terminal operation is called the underlying stream will be replaced with the result of calling the
 * same method on that stream. The return value of these operations is always this stream or, in case of operations
 * that return a different type of stream, one of {@link ParallelStreamSupport}, {@link ParallelIntStreamSupport} or
 * {@link ParallelDoubleStreamSupport}.
 * </p>
 * <p>
 * Although each factory method returns a parallel stream, calling {@link #sequential()} is still possible and leads to
 * sequential execution of a terminal operation within the calling thread.
 * </p>
 * @implNote
 * <p>
 * See the class documentation for {@link Stream} and the package documentation for
 * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html">java.util.stream</a> for
 * additional specification.
 * </p>
 */
public class ParallelLongStreamSupport extends AbstractParallelStreamSupport<Long, LongStream> implements LongStream {

  /**
   * Constructor for internal use within this package only.
   *
   * @param delegate Stream to delegate each operation.
   * @param workerPool Worker pool for executing terminal operations in parallel. Must not be null.
   */
  ParallelLongStreamSupport(LongStream delegate, ForkJoinPool workerPool) {
    super(delegate, workerPool);
  }

  /**
   * Creates a <strong>parallel</strong> {@code long} stream from the given Array. This operation is similar to calling
   * {@code Arrays.stream(array).parallel()} with the difference that a parallel
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
   * operation</a> will be executed in the given {@link ForkJoinPool}.
   *
   * @param array Array to create the parallel stream from. Must not be null.
   * @param workerPool Thread pool for parallel execution of a terminal operation. Must not be null.
   * @return A parallel {@code long} stream that executes a terminal operation in the given {@link ForkJoinPool}.
   * @see Arrays#stream(long[])
   */
  public static LongStream parallelStream(long[] array, ForkJoinPool workerPool) {
    requireNonNull(array, "Array must not be null");

    return new ParallelLongStreamSupport(stream(array).parallel(), workerPool);
  }

  /**
   * Creates a <strong>parallel</strong> {@code long} stream from the given Spliterator. This operation is similar to
   * calling {@code StreamSupport.longStream(spliterator, true)} with the difference that a parallel
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
   * operation</a> will be executed in the given {@link ForkJoinPool}.
   *
   * @param spliterator A {@code Spliterator.OfLong} describing the stream elements. Must not be null.
   * @param workerPool Thread pool for parallel execution of a terminal operation. Must not be null.
   * @return A parallel {@code long} stream that executes a terminal operation in the given {@link ForkJoinPool}.
   * @see StreamSupport#longStream(Spliterator.OfLong, boolean)
   */
  public static LongStream parallelStream(Spliterator.OfLong spliterator, ForkJoinPool workerPool) {
    requireNonNull(spliterator, "Spliterator must not be null");

    return new ParallelLongStreamSupport(longStream(spliterator, true), workerPool);
  }

  /**
   * Creates a <strong>parallel</strong> {@code long} stream from the given Spliterator supplier. This operation is
   * similar to calling {@code StreamSupport.longStream(supplier, characteristics, true)} with the difference that a
   * parallel
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
   * operation</a> will be executed in the given {@link ForkJoinPool}.
   *
   * @param supplier A {@code Supplier} of a {@code Spliterator.OfLong}. Must not be null.
   * @param characteristics Spliterator characteristics of the supplied {@code Spliterator}. The characteristics must
   * be equal to {@code supplier.get().characteristics()}, otherwise undefined behavior may occur when terminal
   * operation commences.
   * @param workerPool Thread pool for parallel execution of a terminal operation. Must not be null.
   * @return A parallel {@code long} stream that executes a terminal operation in the given {@link ForkJoinPool}.
   * @see StreamSupport#longStream(Supplier, int, boolean)
   */
  public static LongStream parallelStream(Supplier<? extends Spliterator.OfLong> supplier, int characteristics, ForkJoinPool workerPool) {
    requireNonNull(supplier, "Supplier must not be null");

    return new ParallelLongStreamSupport(longStream(supplier, characteristics, true), workerPool);
  }

  /**
   * Creates a <strong>parallel</strong> {@code long} stream from the given {@link Builder}. This operation is similar
   * to calling {@code builder.build().parallel()} with the difference that a parallel
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
   * operation</a> will be executed in the given {@link ForkJoinPool}.
   *
   * @param builder The builder to create the stream from. Must not be null.
   * @param workerPool Thread pool for parallel execution of a terminal operation. Must not be null.
   * @return A parallel {@code long} stream that executes a terminal operation in the given {@link ForkJoinPool}.
   * builder()
   */
  public static LongStream parallelStream(Builder builder, ForkJoinPool workerPool) {
    requireNonNull(builder, "Builder must not be null");

    return new ParallelLongStreamSupport(builder.build().parallel(), workerPool);
  }

  /**
   * Creates a <strong>parallel</strong> infinite ordered {@code long} stream produced by iterative application of a
   * function {@code f} to an initial element {@code seed}. This operation is similar to calling
   * {@code LongStream.iterate(seed, operator).parallel()} with the difference that a parallel
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
   * operation</a> will be executed in the given {@link ForkJoinPool}.
   *
   * @param seed The initial element.
   * @param operator A function to be applied to to the previous element to produce a new element. Must not be null.
   * @param workerPool Thread pool for parallel execution of a terminal operation. Must not be null.
   * @return A parallel {@code long} stream that executes a terminal operation in the given {@link ForkJoinPool}.
   * @see LongStream#iterate(long, LongUnaryOperator)
   */
  public static LongStream iterate(long seed, LongUnaryOperator operator, ForkJoinPool workerPool) {
    requireNonNull(operator, "Operator must not be null");

    return new ParallelLongStreamSupport(LongStream.iterate(seed, operator).parallel(), workerPool);
  }

  /**
   * Creates a <strong>parallel</strong> infinite sequential unordered {@code long} stream where each element is
   * generated by the provided {@code LongSupplier}. This operation is similar to calling
   * {@code LongStream.generate(supplier).parallel()} with the difference that a parallel
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
   * operation</a> will be executed in the given {@link ForkJoinPool}.
   *
   * @param supplier The {@code LongSupplier} of generated elements. Must not be null.
   * @param workerPool Thread pool for parallel execution of a terminal operation. Must not be null.
   * @return A parallel {@code long} stream that executes a terminal operation in the given {@link ForkJoinPool}.
   * @see LongStream#generate(LongSupplier)
   */
  public static LongStream generate(LongSupplier supplier, ForkJoinPool workerPool) {
    requireNonNull(supplier, "Supplier must not be null");

    return new ParallelLongStreamSupport(LongStream.generate(supplier).parallel(), workerPool);
  }

  /**
   * Creates a <strong>parallel</strong> ordered {@code long} stream from {@code startInclusive} (inclusive) to
   * {@code endExclusive} (exclusive) by an incremental step of {@code 1}. This operation is similar to calling
   * {@code LongStream.range(startInclusive, endExclusive).parallel()} with the difference that a parallel
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
   * operation</a> will be executed in the given {@link ForkJoinPool}.
   *
   * @param startInclusive the (inclusive) initial value
   * @param endExclusive the exclusive upper bound
   * @param workerPool Thread pool for parallel execution of a terminal operation. Must not be null.
   * @return A parallel {@code long} stream that executes a terminal operation in the given {@link ForkJoinPool} for
   * the range of {@code long} elements.
   * @see LongStream#range(long, long)
   */
  public static LongStream range(long startInclusive, long endExclusive, ForkJoinPool workerPool) {
    return new ParallelLongStreamSupport(LongStream.range(startInclusive, endExclusive).parallel(), workerPool);
  }

  /**
   * Creates a <strong>parallel</strong> ordered {@code long} stream from {@code startInclusive} (inclusive) to
   * {@code endInclusive} (inclusive) by an incremental step of {@code 1}. This operation is similar to calling
   * {@code LongStream.rangeClosed(startInclusive, endInclusive).parallel()} with the difference that a parallel
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
   * operation</a> will be executed in the given {@link ForkJoinPool}.
   *
   * @param startInclusive the (inclusive) initial value
   * @param endInclusive the inclusive upper bound
   * @param workerPool Thread pool for parallel execution of a terminal operation. Must not be null.
   * @return A parallel {@code long} stream that executes a terminal operation in the given {@link ForkJoinPool} for
   * the range of {@code long} elements.
   * @see LongStream#rangeClosed(long, long)
   */
  public static LongStream rangeClosed(long startInclusive, long endInclusive, ForkJoinPool workerPool) {
    return new ParallelLongStreamSupport(LongStream.rangeClosed(startInclusive, endInclusive).parallel(), workerPool);
  }

  /**
   * Creates a lazily concatenated <strong>parallel</strong> {@code long} stream whose elements are all the elements of
   * the first stream followed by all the elements of the second stream. This operation is similar to calling
   * {@code LongStream.concat(a, b).parallel()} with the difference that a parallel
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
   * operation</a> will be executed in the given {@link ForkJoinPool}.
   *
   * @param a The first stream
   * @param b The second stream
   * @param workerPool Thread pool for parallel execution of a terminal operation. Must not be null.
   * @return A parallel stream that executes a terminal operation in the given {@link ForkJoinPool}.
   * @see LongStream#concat(LongStream, LongStream)
   */
  public static LongStream concat(LongStream a, LongStream b, ForkJoinPool workerPool) {
    requireNonNull(a, "Stream a must not be null");
    requireNonNull(b, "Stream b must not be null");

    return new ParallelLongStreamSupport(LongStream.concat(a, b).parallel(), workerPool);
  }

  @Override
  public LongStream filter(LongPredicate predicate) {
    this.delegate = this.delegate.filter(predicate);
    return this;
  }

  @Override
  public LongStream map(LongUnaryOperator mapper) {
    this.delegate = this.delegate.map(mapper);
    return this;
  }

  @Override
  public <U> Stream<U> mapToObj(LongFunction<? extends U> mapper) {
    return new ParallelStreamSupport<>(this.delegate.mapToObj(mapper), this.workerPool);
  }

  @Override
  public IntStream mapToInt(LongToIntFunction mapper) {
    return new ParallelIntStreamSupport(this.delegate.mapToInt(mapper), this.workerPool);
  }

  @Override
  public DoubleStream mapToDouble(LongToDoubleFunction mapper) {
    return new ParallelDoubleStreamSupport(this.delegate.mapToDouble(mapper), this.workerPool);
  }

  @Override
  public LongStream flatMap(LongFunction<? extends LongStream> mapper) {
    this.delegate = this.delegate.flatMap(mapper);
    return this;
  }

  @Override
  public LongStream distinct() {
    this.delegate = this.delegate.distinct();
    return this;
  }

  @Override
  public LongStream sorted() {
    this.delegate = this.delegate.sorted();
    return this;
  }

  @Override
  public LongStream peek(LongConsumer action) {
    this.delegate = this.delegate.peek(action);
    return this;
  }

  @Override
  public LongStream limit(long maxSize) {
    this.delegate = this.delegate.limit(maxSize);
    return this;
  }

  @Override
  public LongStream skip(long n) {
    this.delegate = this.delegate.skip(n);
    return this;
  }

  @Override
  public void forEach(LongConsumer action) {
    execute(() -> this.delegate.forEach(action));
  }

  @Override
  public void forEachOrdered(LongConsumer action) {
    execute(() -> this.delegate.forEachOrdered(action));
  }

  @Override
  public long[] toArray() {
    return execute(() -> this.delegate.toArray());
  }

  @Override
  public long reduce(long identity, LongBinaryOperator op) {
    return execute(() -> this.delegate.reduce(identity, op));
  }

  @Override
  public OptionalLong reduce(LongBinaryOperator op) {
    return execute(() -> this.delegate.reduce(op));
  }

  @Override
  public <R> R collect(Supplier<R> supplier, ObjLongConsumer<R> accumulator, BiConsumer<R, R> combiner) {
    return execute(() -> this.delegate.collect(supplier, accumulator, combiner));
  }

  @Override
  public long sum() {
    return execute(() -> this.delegate.sum());
  }

  @Override
  public OptionalLong min() {
    return execute(() -> this.delegate.min());
  }

  @Override
  public OptionalLong max() {
    return execute(() -> this.delegate.max());
  }

  @Override
  public long count() {
    return execute(() -> this.delegate.count());
  }

  @Override
  public OptionalDouble average() {
    return execute(() -> this.delegate.average());
  }

  @Override
  public LongSummaryStatistics summaryStatistics() {
    return execute(() -> this.delegate.summaryStatistics());
  }

  @Override
  public boolean anyMatch(LongPredicate predicate) {
    return execute(() -> this.delegate.anyMatch(predicate));
  }

  @Override
  public boolean allMatch(LongPredicate predicate) {
    return execute(() -> this.delegate.allMatch(predicate));
  }

  @Override
  public boolean noneMatch(LongPredicate predicate) {
    return execute(() -> this.delegate.noneMatch(predicate));
  }

  @Override
  public OptionalLong findFirst() {
    return execute(() -> this.delegate.findFirst());
  }

  @Override
  public OptionalLong findAny() {
    return execute(() -> this.delegate.findAny());
  }

  @Override
  public DoubleStream asDoubleStream() {
    return new ParallelDoubleStreamSupport(this.delegate.asDoubleStream(), this.workerPool);
  }

  @Override
  public Stream<Long> boxed() {
    return new ParallelStreamSupport<>(this.delegate.boxed(), this.workerPool);
  }

  @Override
  public OfLong iterator() {
    return this.delegate.iterator();
  }

  @Override
  public java.util.Spliterator.OfLong spliterator() {
    return this.delegate.spliterator();
  }


}
