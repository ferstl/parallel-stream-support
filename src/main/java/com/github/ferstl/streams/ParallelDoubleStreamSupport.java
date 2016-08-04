package com.github.ferstl.streams;

import java.util.Arrays;
import java.util.DoubleSummaryStatistics;
import java.util.OptionalDouble;
import java.util.PrimitiveIterator.OfDouble;
import java.util.Spliterator;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.function.BiConsumer;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleConsumer;
import java.util.function.DoubleFunction;
import java.util.function.DoublePredicate;
import java.util.function.DoubleSupplier;
import java.util.function.DoubleToIntFunction;
import java.util.function.DoubleToLongFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.function.ObjDoubleConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.stream.StreamSupport.doubleStream;

/**
 * <p>
 * An implementation of {@link DoubleStream} which uses a custom {@link ForkJoinPool} for parallel aggregate
 * operations. This is the {@code double} primitive specialization of {@link ParallelStreamSupport}.
 * <p>
 * The following example illustrates an aggregate operation using {@link ParallelStreamSupport} and
 * {@link ParallelDoubleStreamSupport} with a custom {@link ForkJoinPool}, computing the sum of the weights of the red
 * widgets:
 *
 * <pre>
 *
 * ForkJoinPool pool = new ForkJoinPool();
 * double sum = ParallelStreamSupport.parallelStream(widgets, pool)
 *     .filter(w -&gt; w.getColor() == RED)
 *     .mapToDouble(w -&gt; w.getWeight())
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
 * <li>The static factory methods of {@link DoubleStream}, which are meaningful for parallel streams</li>
 * <li>{@link Arrays#stream(double[])}</li>
 * <li>{@link StreamSupport#doubleStream(Spliterator.OfDouble, boolean)}</li>
 * <li>{@link StreamSupport#doubleStream(Supplier, int, boolean)}</li>
 * </ul>
 *
 * @apiNote
 * <p>
 * Internally, this stream wraps a {@code double} stream which is initially created in one of the static factory
 * methods. Whenever a non-terminal operation is called the underlying stream will be replaced with the result of
 * calling the same method on that stream. The return value of these operations is always this stream or, in case of
 * operations that return a different type of stream, one of {@link ParallelStreamSupport},
 * {@link ParallelIntStreamSupport} or {@link ParallelLongStreamSupport}.
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
public class ParallelDoubleStreamSupport extends AbstractParallelStreamSupport<Double, DoubleStream> implements DoubleStream {

  /**
   * Constructor for internal use within this package only.
   *
   * @param delegate Stream to delegate each operation.
   * @param workerPool Worker pool for executing terminal operations in parallel. Must not be null.
   */
  ParallelDoubleStreamSupport(DoubleStream delegate, ForkJoinPool workerPool) {
    super(delegate, workerPool);
  }

  /**
   * Creates a <strong>parallel</strong> {@code double} stream from the given Array. This operation is similar to
   * calling {@code Arrays.stream(array).parallel()} with the difference that a parallel
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
   * operation</a> will be executed in the given {@link ForkJoinPool}.
   *
   * @param array Array to create the parallel stream from. Must not be null.
   * @param workerPool Thread pool for parallel execution of a terminal operation. Must not be null.
   * @return A parallel {@code double} stream that executes a terminal operation in the given {@link ForkJoinPool}.
   * @see Arrays#stream(int[])
   */
  public static DoubleStream parallelStream(double[] array, ForkJoinPool workerPool) {
    requireNonNull(array, "Array must not be null");

    return new ParallelDoubleStreamSupport(stream(array).parallel(), workerPool);
  }

  /**
   * Creates a <strong>parallel</strong> {@code double} stream from the given Spliterator. This operation is similar to
   * calling {@code StreamSupport.doubleStream(spliterator, true)} with the difference that a parallel
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
   * operation</a> will be executed in the given {@link ForkJoinPool}.
   *
   * @param spliterator A {@code Spliterator.OfDouble} describing the stream elements. Must not be null.
   * @param workerPool Thread pool for parallel execution of a terminal operation. Must not be null.
   * @return A parallel {@code double} stream that executes a terminal operation in the given {@link ForkJoinPool}.
   * @see StreamSupport#doubleStream(Spliterator.OfDouble, boolean)
   */
  public static DoubleStream parallelStream(Spliterator.OfDouble spliterator, ForkJoinPool workerPool) {
    requireNonNull(spliterator, "Spliterator must not be null");

    return new ParallelDoubleStreamSupport(doubleStream(spliterator, true), workerPool);
  }

  /**
   * Creates a <strong>parallel</strong> {@code double} stream from the given Spliterator supplier. This operation is
   * similar to calling {@code StreamSupport.doubleStream(supplier, characteristics, true)} with the difference that a
   * parallel
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
   * operation</a> will be executed in the given {@link ForkJoinPool}.
   *
   * @param supplier A {@code Supplier} of a {@code Spliterator.OfDouble}. Must not be null.
   * @param characteristics Spliterator characteristics of the supplied {@code Spliterator}. The characteristics must
   * be equal to {@code supplier.get().characteristics()}, otherwise undefined behavior may occur when terminal
   * operation commences.
   * @param workerPool Thread pool for parallel execution of a terminal operation. Must not be null.
   * @return A parallel {@code double} stream that executes a terminal operation in the given {@link ForkJoinPool}.
   * @see StreamSupport#doubleStream(Supplier, int, boolean)
   */
  public static DoubleStream parallelStream(Supplier<? extends Spliterator.OfDouble> supplier, int characteristics, ForkJoinPool workerPool) {
    requireNonNull(supplier, "Supplier must not be null");

    return new ParallelDoubleStreamSupport(doubleStream(supplier, characteristics, true), workerPool);
  }

  /**
   * Creates a <strong>parallel</strong> {@code double} stream from the given {@link Builder}. This operation is
   * similar to calling {@code builder.build().parallel()} with the difference that a parallel
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
   * operation</a> will be executed in the given {@link ForkJoinPool}.
   *
   * @param builder The builder to create the stream from. Must not be null.
   * @param workerPool Thread pool for parallel execution of a terminal operation. Must not be null.
   * @return A parallel {@code double} stream that executes a terminal operation in the given {@link ForkJoinPool}.
   * @see DoubleStream#builder()
   */
  public static DoubleStream parallelStream(Builder builder, ForkJoinPool workerPool) {
    requireNonNull(builder, "Builder must not be null");

    return new ParallelDoubleStreamSupport(builder.build().parallel(), workerPool);
  }

  /**
   * Creates a <strong>parallel</strong> infinite ordered {@code double} stream produced by iterative application of a
   * function {@code f} to an initial element {@code seed}. This operation is similar to calling
   * {@code DoubleStream.iterate(seed, operator).parallel()} with the difference that a parallel
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
   * operation</a> will be executed in the given {@link ForkJoinPool}.
   *
   * @param seed The initial element.
   * @param operator A function to be applied to to the previous element to produce a new element. Must not be null.
   * @param workerPool Thread pool for parallel execution of a terminal operation. Must not be null.
   * @return A parallel {@code double} stream that executes a terminal operation in the given {@link ForkJoinPool}.
   * @see DoubleStream#iterate(double, DoubleUnaryOperator)
   */
  public static DoubleStream iterate(double seed, DoubleUnaryOperator operator, ForkJoinPool workerPool) {
    requireNonNull(operator, "Operator must not be null");

    return new ParallelDoubleStreamSupport(DoubleStream.iterate(seed, operator).parallel(), workerPool);
  }

  /**
   * Creates a <strong>parallel</strong> infinite sequential unordered {@code double} stream where each element is
   * generated by the provided {@code DoubleSupplier}. This operation is similar to calling
   * {@code DoubleStream.generate(supplier).parallel()} with the difference that a parallel
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
   * operation</a> will be executed in the given {@link ForkJoinPool}.
   *
   * @param supplier The {@code DoubleSupplier} of generated elements. Must not be null.
   * @param workerPool Thread pool for parallel execution of a terminal operation. Must not be null.
   * @return A parallel {@code double} stream that executes a terminal operation in the given {@link ForkJoinPool}.
   * @see DoubleStream#generate(DoubleSupplier)
   */
  public static DoubleStream generate(DoubleSupplier supplier, ForkJoinPool workerPool) {
    requireNonNull(supplier, "Supplier must not be null");

    return new ParallelDoubleStreamSupport(DoubleStream.generate(supplier).parallel(), workerPool);
  }

  /**
   * Creates a lazily concatenated <strong>parallel</strong> {@code double} stream whose elements are all the elements
   * of the first stream followed by all the elements of the second stream. This operation is similar to calling
   * {@code DoubleStream.concat(a, b).parallel()} with the difference that a parallel
   * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
   * operation</a> will be executed in the given {@link ForkJoinPool}.
   *
   * @param a The first stream. Must not be null.
   * @param b The second stream. Must not be null.
   * @param workerPool Thread pool for parallel execution of a terminal operation. Must not be null.
   * @return A parallel stream that executes a terminal operation in the given {@link ForkJoinPool}.
   * @see DoubleStream#concat(DoubleStream, DoubleStream)
   */
  public static DoubleStream concat(DoubleStream a, DoubleStream b, ForkJoinPool workerPool) {
    requireNonNull(a, "Stream a must not be null");
    requireNonNull(b, "Stream b must not be null");

    return new ParallelDoubleStreamSupport(DoubleStream.concat(a, b).parallel(), workerPool);
  }

  @Override
  public DoubleStream filter(DoublePredicate predicate) {
    this.delegate = this.delegate.filter(predicate);
    return this;
  }

  @Override
  public DoubleStream map(DoubleUnaryOperator mapper) {
    this.delegate = this.delegate.map(mapper);
    return this;
  }

  @Override
  public <U> Stream<U> mapToObj(DoubleFunction<? extends U> mapper) {
    return new ParallelStreamSupport<>(this.delegate.mapToObj(mapper), this.workerPool);
  }

  @Override
  public IntStream mapToInt(DoubleToIntFunction mapper) {
    return new ParallelIntStreamSupport(this.delegate.mapToInt(mapper), this.workerPool);
  }

  @Override
  public LongStream mapToLong(DoubleToLongFunction mapper) {
    return new ParallelLongStreamSupport(this.delegate.mapToLong(mapper), this.workerPool);
  }

  @Override
  public DoubleStream flatMap(DoubleFunction<? extends DoubleStream> mapper) {
    this.delegate = this.delegate.flatMap(mapper);
    return this;
  }

  @Override
  public DoubleStream distinct() {
    this.delegate = this.delegate.distinct();
    return this;
  }

  @Override
  public DoubleStream sorted() {
    this.delegate = this.delegate.sorted();
    return this;
  }

  @Override
  public DoubleStream peek(DoubleConsumer action) {
    this.delegate = this.delegate.peek(action);
    return this;
  }

  @Override
  public DoubleStream limit(long maxSize) {
    this.delegate = this.delegate.limit(maxSize);
    return this;
  }

  @Override
  public DoubleStream skip(long n) {
    this.delegate = this.delegate.skip(n);
    return this;
  }

  @Override
  public void forEach(DoubleConsumer action) {
    execute(() -> this.delegate.forEach(action));
  }

  @Override
  public void forEachOrdered(DoubleConsumer action) {
    execute(() -> this.delegate.forEachOrdered(action));
  }

  @Override
  public double[] toArray() {
    return execute(() -> this.delegate.toArray());
  }

  @Override
  public double reduce(double identity, DoubleBinaryOperator op) {
    return execute(() -> this.delegate.reduce(identity, op));
  }

  @Override
  public OptionalDouble reduce(DoubleBinaryOperator op) {
    return execute(() -> this.delegate.reduce(op));
  }

  @Override
  public <R> R collect(Supplier<R> supplier, ObjDoubleConsumer<R> accumulator, BiConsumer<R, R> combiner) {
    return execute(() -> this.delegate.collect(supplier, accumulator, combiner));
  }

  @Override
  public double sum() {
    return execute(() -> this.delegate.sum());
  }

  @Override
  public OptionalDouble min() {
    return execute(() -> this.delegate.min());
  }

  @Override
  public OptionalDouble max() {
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
  public DoubleSummaryStatistics summaryStatistics() {
    return execute(() -> this.delegate.summaryStatistics());
  }

  @Override
  public boolean anyMatch(DoublePredicate predicate) {
    return execute(() -> this.delegate.anyMatch(predicate));
  }

  @Override
  public boolean allMatch(DoublePredicate predicate) {
    return execute(() -> this.delegate.allMatch(predicate));
  }

  @Override
  public boolean noneMatch(DoublePredicate predicate) {
    return execute(() -> this.delegate.noneMatch(predicate));
  }

  @Override
  public OptionalDouble findFirst() {
    return execute(() -> this.delegate.findFirst());
  }

  @Override
  public OptionalDouble findAny() {
    return execute(() -> this.delegate.findAny());
  }

  @Override
  public Stream<Double> boxed() {
    return new ParallelStreamSupport<>(this.delegate.boxed(), this.workerPool);
  }

  @Override
  public OfDouble iterator() {
    return this.delegate.iterator();
  }

  @Override
  public java.util.Spliterator.OfDouble spliterator() {
    return this.delegate.spliterator();
  }

}
