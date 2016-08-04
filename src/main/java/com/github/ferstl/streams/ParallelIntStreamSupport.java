package com.github.ferstl.streams;

import java.util.Arrays;
import java.util.IntSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.PrimitiveIterator.OfInt;
import java.util.Spliterator;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.function.BiConsumer;
import java.util.function.IntBinaryOperator;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.IntSupplier;
import java.util.function.IntToDoubleFunction;
import java.util.function.IntToLongFunction;
import java.util.function.IntUnaryOperator;
import java.util.function.ObjIntConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.stream.StreamSupport.intStream;


/**
 * <p>
 * An implementation of {@link IntStream} which uses a custom {@link ForkJoinPool} for parallel aggregate operations.
 * This is the {@code int} primitive specialization of {@link ParallelStreamSupport}.
 * <p>
 * The following example illustrates an aggregate operation using {@link ParallelStreamSupport} and
 * {@link ParallelIntStreamSupport} with a custom {@link ForkJoinPool}, computing the sum of the weights of the red
 * widgets:
 *
 * <pre>
 *
 * ForkJoinPool pool = new ForkJoinPool();
 * int sum = ParallelStreamSupport.parallelStream(widgets, pool)
 *     .filter(w -&gt; w.getColor() == RED)
 *     .mapToInt(w -&gt; w.getWeight())
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
 * <li>The static factory methods of {@link IntStream}, which are meaningful for parallel streams</li>
 * <li>{@link Arrays#stream(int[])}</li>
 * <li>{@link StreamSupport#intStream(Spliterator.OfInt, boolean)}</li>
 * <li>{@link StreamSupport#intStream(Supplier, int, boolean)}</li>
 * </ul>
 *
 * @apiNote
 * <p>
 * Internally, this stream wraps an {@code int} stream which is initially created in one of the static factory methods.
 * Whenever a non-terminal operation is called the underlying stream will be replaced with the result of calling the
 * same method on that stream. The return value of these operations is always this stream or, in case of operations
 * that return a different type of stream, one of {@link ParallelStreamSupport}, {@link ParallelLongStreamSupport} or
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
public class ParallelIntStreamSupport extends AbstractParallelStreamSupport<Integer, IntStream> implements IntStream {

  /**
   * Constructor for internal use within this package only.
   *
   * @param delegate Stream to delegate each operation.
   * @param workerPool Worker pool for executing terminal operations in parallel. Must not be null.
   */
  ParallelIntStreamSupport(IntStream delegate, ForkJoinPool workerPool) {
    super(delegate, workerPool);
  }

  /**
   * Creates a <strong>parallel</strong> {@code int} stream from the given Array. This operation is similar to calling
   * {@code Arrays.stream(array).parallel()} with the difference that a parallel
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
   * operation</a> will be executed in the given {@link ForkJoinPool}.
   *
   * @param array Array to create the parallel stream from. Must not be null.
   * @param workerPool Thread pool for parallel execution of a terminal operation. Must not be null.
   * @return A parallel {@code int} stream that executes a terminal operation in the given {@link ForkJoinPool}.
   * @see Arrays#stream(int[])
   */
  public static IntStream parallelStream(int[] array, ForkJoinPool workerPool) {
    requireNonNull(array, "Array must not be null");

    return new ParallelIntStreamSupport(stream(array).parallel(), workerPool);
  }

  /**
   * Creates a <strong>parallel</strong> {@code int} stream from the given Spliterator. This operation is similar to
   * calling {@code StreamSupport.intStream(spliterator, true)} with the difference that a parallel
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
   * operation</a> will be executed in the given {@link ForkJoinPool}.
   *
   * @param spliterator A {@code Spliterator.OfInt} describing the stream elements. Must not be null.
   * @param workerPool Thread pool for parallel execution of a terminal operation. Must not be null.
   * @return A parallel {@code int} stream that executes a terminal operation in the given {@link ForkJoinPool}.
   * @see StreamSupport#intStream(Spliterator.OfInt, boolean)
   */
  public static IntStream parallelStream(Spliterator.OfInt spliterator, ForkJoinPool workerPool) {
    requireNonNull(spliterator, "Spliterator must not be null");

    return new ParallelIntStreamSupport(intStream(spliterator, true), workerPool);
  }

  /**
   * Creates a <strong>parallel</strong> {@code int} stream from the given Spliterator supplier. This operation is
   * similar to calling {@code StreamSupport.intStream(supplier, characteristics, true)} with the difference that a
   * parallel
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
   * operation</a> will be executed in the given {@link ForkJoinPool}.
   *
   * @param supplier A {@code Supplier} of a {@code Spliterator.OfInt}. Must not be null.
   * @param characteristics Spliterator characteristics of the supplied {@code Spliterator}. The characteristics must
   * be equal to {@code supplier.get().characteristics()}, otherwise undefined behavior may occur when terminal
   * operation commences.
   * @param workerPool Thread pool for parallel execution of a terminal operation. Must not be null.
   * @return A parallel {@code int} stream that executes a terminal operation in the given {@link ForkJoinPool}.
   * @see StreamSupport#intStream(Supplier, int, boolean)
   */
  public static IntStream parallelStream(Supplier<? extends Spliterator.OfInt> supplier, int characteristics, ForkJoinPool workerPool) {
    requireNonNull(supplier, "Supplier must not be null");

    return new ParallelIntStreamSupport(intStream(supplier, characteristics, true), workerPool);
  }

  /**
   * Creates a <strong>parallel</strong> {@code int} stream from the given {@link Builder}. This operation is similar
   * to calling {@code builder.build().parallel()} with the difference that a parallel
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
   * operation</a> will be executed in the given {@link ForkJoinPool}.
   *
   * @param builder The builder to create the stream from. Must not be null.
   * @param workerPool Thread pool for parallel execution of a terminal operation. Must not be null.
   * @return A parallel {@code int} stream that executes a terminal operation in the given {@link ForkJoinPool}.
   * @see IntStream#builder()
   */
  public static IntStream parallelStream(Builder builder, ForkJoinPool workerPool) {
    requireNonNull(builder, "Builder must not be null");

    return new ParallelIntStreamSupport(builder.build().parallel(), workerPool);
  }

  /**
   * Creates a <strong>parallel</strong> infinite ordered {@code int} stream produced by iterative application of a
   * function {@code f} to an initial element {@code seed}. This operation is similar to calling
   * {@code IntStream.iterate(seed, operator).parallel()} with the difference that a parallel
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
   * operation</a> will be executed in the given {@link ForkJoinPool}.
   *
   * @param seed The initial element.
   * @param operator A function to be applied to to the previous element to produce a new element
   * @param workerPool Thread pool for parallel execution of a terminal operation. Must not be null.
   * @return A parallel {@code int} stream that executes a terminal operation in the given {@link ForkJoinPool}.
   * @see IntStream#iterate(int, IntUnaryOperator)
   */
  public static IntStream iterate(int seed, IntUnaryOperator operator, ForkJoinPool workerPool) {
    requireNonNull(operator, "Operator must not be null");

    return new ParallelIntStreamSupport(IntStream.iterate(seed, operator).parallel(), workerPool);
  }

  /**
   * Creates a <strong>parallel</strong> infinite sequential unordered {@code int} stream where each element is
   * generated by the provided {@code Supplier}. This operation is similar to calling
   * {@code IntStream.generate(supplier).parallel()} with the difference that a parallel
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
   * operation</a> will be executed in the given {@link ForkJoinPool}.
   *
   * @param supplier The {@code IntSupplier} of generated elements.
   * @param workerPool Thread pool for parallel execution of a terminal operation. Must not be null.
   * @return A parallel {@code int} stream that executes a terminal operation in the given {@link ForkJoinPool}.
   * @see IntStream#generate(IntSupplier)
   */
  public static IntStream generate(IntSupplier supplier, ForkJoinPool workerPool) {
    requireNonNull(supplier, "Supplier must not be null");

    return new ParallelIntStreamSupport(IntStream.generate(supplier).parallel(), workerPool);
  }

  /**
   * Creates a <strong>parallel</strong> ordered {@code int} stream from {@code startInclusive} (inclusive) to
   * {@code endExclusive} (exclusive) by an incremental step of {@code 1}. This operation is similar to calling
   * {@code IntStream.range(startInclusive, endExclusive).parallel()} with the difference that a parallel
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
   * operation</a> will be executed in the given {@link ForkJoinPool}.
   *
   * @param startInclusive the (inclusive) initial value
   * @param endExclusive the exclusive upper bound
   * @param workerPool Thread pool for parallel execution of a terminal operation. Must not be null.
   * @return A parallel {@code int} stream that executes a terminal operation in the given {@link ForkJoinPool} for the
   * range of {@code int} elements.
   * @see IntStream#range(int, int)
   */
  public static IntStream range(int startInclusive, int endExclusive, ForkJoinPool workerPool) {
    return new ParallelIntStreamSupport(IntStream.range(startInclusive, endExclusive).parallel(), workerPool);
  }

  /**
   * Creates a <strong>parallel</strong> ordered {@code int} stream from {@code startInclusive} (inclusive) to
   * {@code endInclusive} (inclusive) by an incremental step of {@code 1}. This operation is similar to calling
   * {@code IntStream.rangeClosed(startInclusive, endInclusive).parallel()} with the difference that a parallel
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
   * operation</a> will be executed in the given {@link ForkJoinPool}.
   *
   * @param startInclusive the (inclusive) initial value
   * @param endInclusive the inclusive upper bound
   * @param workerPool Thread pool for parallel execution of a terminal operation. Must not be null.
   * @return A parallel {@code int} stream that executes a terminal operation in the given {@link ForkJoinPool} for the
   * range of {@code int} elements.
   * @see IntStream#rangeClosed(int, int)
   */
  public static IntStream rangeClosed(int startInclusive, int endInclusive, ForkJoinPool workerPool) {
    return new ParallelIntStreamSupport(IntStream.rangeClosed(startInclusive, endInclusive).parallel(), workerPool);
  }

  /**
   * Creates a lazily concatenated <strong>parallel</strong> {@code int} stream whose elements are all the elements of
   * the first stream followed by all the elements of the second stream. This operation is similar to calling
   * {@code IntStream.concat(a, b).parallel()} with the difference that a parallel
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
   * operation</a> will be executed in the given {@link ForkJoinPool}.
   *
   * @param a The first stream
   * @param b The second stream
   * @param workerPool Thread pool for parallel execution of a terminal operation. Must not be null.
   * @return A parallel stream that executes a terminal operation in the given {@link ForkJoinPool}.
   * @see IntStream#concat(IntStream, IntStream)
   */
  public static IntStream concat(IntStream a, IntStream b, ForkJoinPool workerPool) {
    requireNonNull(a, "Stream a must not be null");
    requireNonNull(b, "Stream b must not be null");

    return new ParallelIntStreamSupport(IntStream.concat(a, b).parallel(), workerPool);
  }

  @Override
  public IntStream filter(IntPredicate predicate) {
    this.delegate = this.delegate.filter(predicate);
    return this;
  }

  @Override
  public IntStream map(IntUnaryOperator mapper) {
    this.delegate = this.delegate.map(mapper);
    return this;
  }

  @Override
  public <U> Stream<U> mapToObj(IntFunction<? extends U> mapper) {
    return new ParallelStreamSupport<U>(this.delegate.mapToObj(mapper), this.workerPool);
  }

  @Override
  public LongStream mapToLong(IntToLongFunction mapper) {
    return new ParallelLongStreamSupport(this.delegate.mapToLong(mapper), this.workerPool);
  }

  @Override
  public DoubleStream mapToDouble(IntToDoubleFunction mapper) {
    return new ParallelDoubleStreamSupport(this.delegate.mapToDouble(mapper), this.workerPool);
  }

  @Override
  public IntStream flatMap(IntFunction<? extends IntStream> mapper) {
    this.delegate = this.delegate.flatMap(mapper);
    return this;
  }

  @Override
  public IntStream distinct() {
    this.delegate = this.delegate.distinct();
    return this;
  }

  @Override
  public IntStream sorted() {
    this.delegate = this.delegate.sorted();
    return this;
  }

  @Override
  public IntStream peek(IntConsumer action) {
    this.delegate = this.delegate.peek(action);
    return this;
  }

  @Override
  public IntStream limit(long maxSize) {
    this.delegate = this.delegate.limit(maxSize);
    return this;
  }

  @Override
  public IntStream skip(long n) {
    this.delegate = this.delegate.skip(n);
    return this;
  }

  @Override
  public void forEach(IntConsumer action) {
    execute(() -> this.delegate.forEach(action));
  }

  @Override
  public void forEachOrdered(IntConsumer action) {
    execute(() -> this.delegate.forEachOrdered(action));
  }

  @Override
  public int[] toArray() {
    return execute(() -> this.delegate.toArray());
  }

  @Override
  public int reduce(int identity, IntBinaryOperator op) {
    return execute(() -> this.delegate.reduce(identity, op));
  }

  @Override
  public OptionalInt reduce(IntBinaryOperator op) {
    return execute(() -> this.delegate.reduce(op));
  }

  @Override
  public <R> R collect(Supplier<R> supplier, ObjIntConsumer<R> accumulator, BiConsumer<R, R> combiner) {
    return execute(() -> this.delegate.collect(supplier, accumulator, combiner));
  }

  @Override
  public int sum() {
    return execute(() -> this.delegate.sum());
  }

  @Override
  public OptionalInt min() {
    return execute(() -> this.delegate.min());
  }

  @Override
  public OptionalInt max() {
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
  public IntSummaryStatistics summaryStatistics() {
    return execute(() -> this.delegate.summaryStatistics());
  }

  @Override
  public boolean anyMatch(IntPredicate predicate) {
    return execute(() -> this.delegate.anyMatch(predicate));
  }

  @Override
  public boolean allMatch(IntPredicate predicate) {
    return execute(() -> this.delegate.allMatch(predicate));
  }

  @Override
  public boolean noneMatch(IntPredicate predicate) {
    return execute(() -> this.delegate.noneMatch(predicate));
  }

  @Override
  public OptionalInt findFirst() {
    return execute(() -> this.delegate.findFirst());
  }

  @Override
  public OptionalInt findAny() {
    return execute(() -> this.delegate.findAny());
  }

  @Override
  public LongStream asLongStream() {
    return new ParallelLongStreamSupport(this.delegate.asLongStream(), this.workerPool);
  }

  @Override
  public DoubleStream asDoubleStream() {
    return new ParallelDoubleStreamSupport(this.delegate.asDoubleStream(), this.workerPool);
  }

  @Override
  public Stream<Integer> boxed() {
    return new ParallelStreamSupport<>(this.delegate.boxed(), this.workerPool);
  }

  @Override
  public OfInt iterator() {
    return this.delegate.iterator();
  }

  @Override
  public java.util.Spliterator.OfInt spliterator() {
    return this.delegate.spliterator();
  }
}
