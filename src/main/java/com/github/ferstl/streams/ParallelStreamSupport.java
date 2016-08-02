package com.github.ferstl.streams;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.stream.StreamSupport.stream;

/**
 * <p>
 * An implementation of {@link Stream} which uses a custom {@link ForkJoinPool} for parallel aggregations. The
 * following example illustrates an aggregate operation using {@link ParallelStreamSupport} with a custom
 * {@link ForkJoinPool}:
 *
 * <pre>
 *
 * ForkJoinPool pool = new ForkJoinPool();
 * int sum = ParallelStreamSupport.parallelStream(widgets, pool)
 *     .filter(w -> w.getColor() == RED)
 *     .mapToInt(w -> w.getWeight())
 *     .sum();
 * </pre>
 * </p>
 * <p>
 * In case this stream is configured for parallel execution, i.e. {@link #isParallel()} returns {@code true}, a
 * <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html#StreamOps">terminal
 * operation</a> will be executed as {@link ForkJoinTask} in the custom {@link ForkJoinPool}. Otherwise it will be
 * executed in the calling thread.
 * </p>
 * <p>
 * This implementation offers various factory methods which are based on:
 * <ul>
 * <li>The static factory methods of {@link Stream}, which are meaningful for parallel streams</li>
 * <li>{@link Collection#parallelStream()}</li>
 * <li>{@link Arrays#stream(Object[])}</li>
 * <li>{@link StreamSupport#stream(Spliterator, boolean)}</li>
 * <li>{@link StreamSupport#stream(Supplier, int, boolean))}</li>
 * </ul>
 * </p>
 *
 * @apiNote Internally, this stream wraps a stream which is initially created in one of the static factory methods.
 * Whenever a non-terminal operation is called the underlying stream will be replaced with the result of calling the
 * same method on that stream. Although each factory method returns a parallel stream, calling {@link #sequential()} is
 * still possible and leads to sequential execution of a terminal operation within the calling thread.
 */
public class ParallelStreamSupport<T> extends AbstractParallelStreamSupport<T, Stream<T>> implements Stream<T> {

  ParallelStreamSupport(Stream<T> delegate, ForkJoinPool workerPool) {
    super(delegate, workerPool);
  }

  public static <T> Stream<T> parallelStream(Collection<T> collection, ForkJoinPool workerPool) {
    requireNonNull(collection, "Collection must not be null");

    return new ParallelStreamSupport<>(collection.parallelStream(), workerPool);
  }

  public static <T> Stream<T> parallelStream(T[] array, ForkJoinPool workerPool) {
    requireNonNull(array, "Array must not be null");

    return new ParallelStreamSupport<>(stream(array).parallel(), workerPool);
  }

  public static <T> Stream<T> parallelStream(Spliterator<T> spliterator, ForkJoinPool workerPool) {
    requireNonNull(spliterator, "Spliterator must not be null");

    return new ParallelStreamSupport<>(stream(spliterator, true), workerPool);
  }

  public static <T> Stream<T> parallelStream(Supplier<? extends Spliterator<T>> supplier, int characteristics, ForkJoinPool workerPool) {
    requireNonNull(supplier, "Supplier must not be null");

    return new ParallelStreamSupport<>(stream(supplier, characteristics, true), workerPool);
  }

  public static <T> Stream<T> parallelStream(Builder<T> builder, ForkJoinPool workerPool) {
    requireNonNull(builder, "Builder must not be null");

    return new ParallelStreamSupport<>(builder.build().parallel(), workerPool);
  }

  public static <T> Stream<T> iterate(T seed, UnaryOperator<T> operator, ForkJoinPool workerPool) {
    requireNonNull(operator, "Operator must not be null");

    return new ParallelStreamSupport<>(Stream.iterate(seed, operator).parallel(), workerPool);
  }

  public static <T> Stream<T> generate(Supplier<T> supplier, ForkJoinPool workerPool) {
    requireNonNull(supplier, "Supplier must not be null");

    return new ParallelStreamSupport<>(Stream.generate(supplier).parallel(), workerPool);
  }

  public static <T> Stream<T> concat(Stream<? extends T> a, Stream<? extends T> b, ForkJoinPool workerPool) {
    requireNonNull(a, "Stream a must not be null");
    requireNonNull(b, "Stream b must not be null");

    return new ParallelStreamSupport<>(Stream.concat(a, b).parallel(), workerPool);
  }

  @Override
  public Stream<T> filter(Predicate<? super T> predicate) {
    this.delegate = this.delegate.filter(predicate);
    return this;
  }

  @Override
  public <R> Stream<R> map(Function<? super T, ? extends R> mapper) {
    return new ParallelStreamSupport<>(this.delegate.map(mapper), this.workerPool);
  }

  @Override
  public IntStream mapToInt(ToIntFunction<? super T> mapper) {
    return new ParallelIntStreamSupport(this.delegate.mapToInt(mapper), this.workerPool);
  }

  @Override
  public LongStream mapToLong(ToLongFunction<? super T> mapper) {
    return new ParallelLongStreamSupport(this.delegate.mapToLong(mapper), this.workerPool);
  }

  @Override
  public DoubleStream mapToDouble(ToDoubleFunction<? super T> mapper) {
    return new ParallelDoubleStreamSupport(this.delegate.mapToDouble(mapper), this.workerPool);
  }

  @Override
  public <R> Stream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
    return new ParallelStreamSupport<>(this.delegate.flatMap(mapper), this.workerPool);
  }

  @Override
  public IntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper) {
    return new ParallelIntStreamSupport(this.delegate.flatMapToInt(mapper), this.workerPool);
  }

  @Override
  public LongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper) {
    return new ParallelLongStreamSupport(this.delegate.flatMapToLong(mapper), this.workerPool);
  }

  @Override
  public DoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper) {
    return new ParallelDoubleStreamSupport(this.delegate.flatMapToDouble(mapper), this.workerPool);
  }

  @Override
  public Stream<T> distinct() {
    this.delegate = this.delegate.distinct();
    return this;
  }

  @Override
  public Stream<T> sorted() {
    this.delegate = this.delegate.sorted();
    return this;
  }

  @Override
  public Stream<T> sorted(Comparator<? super T> comparator) {
    this.delegate = this.delegate.sorted(comparator);
    return this;
  }

  @Override
  public Stream<T> peek(Consumer<? super T> action) {
    this.delegate = this.delegate.peek(action);
    return this;
  }

  @Override
  public Stream<T> limit(long maxSize) {
    this.delegate = this.delegate.limit(maxSize);
    return this;
  }

  @Override
  public Stream<T> skip(long n) {
    this.delegate = this.delegate.skip(n);
    return this;
  }

  // Terminal operations

  @Override
  public void forEach(Consumer<? super T> action) {
    execute(() -> this.delegate.forEach(action));
  }

  @Override
  public void forEachOrdered(Consumer<? super T> action) {
    execute(() -> this.delegate.forEachOrdered(action));
  }

  @Override
  public Object[] toArray() {
    return execute(() -> this.delegate.toArray());
  }

  @Override
  public <A> A[] toArray(IntFunction<A[]> generator) {
    return execute(() -> this.delegate.toArray(generator));
  }

  @Override
  public T reduce(T identity, BinaryOperator<T> accumulator) {
    return execute(() -> this.delegate.reduce(identity, accumulator));
  }

  @Override
  public Optional<T> reduce(BinaryOperator<T> accumulator) {
    return execute(() -> this.delegate.reduce(accumulator));
  }

  @Override
  public <U> U reduce(U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner) {
    return execute(() -> this.delegate.reduce(identity, accumulator, combiner));
  }

  @Override
  public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
    return execute(() -> this.delegate.collect(supplier, accumulator, combiner));
  }

  @Override
  public <R, A> R collect(Collector<? super T, A, R> collector) {
    return execute(() -> this.delegate.collect(collector));
  }

  @Override
  public Optional<T> min(Comparator<? super T> comparator) {
    return execute(() -> this.delegate.min(comparator));
  }

  @Override
  public Optional<T> max(Comparator<? super T> comparator) {
    return execute(() -> this.delegate.max(comparator));
  }

  @Override
  public long count() {
    return execute(() -> this.delegate.count());
  }

  @Override
  public boolean anyMatch(Predicate<? super T> predicate) {
    return execute(() -> this.delegate.anyMatch(predicate));
  }

  @Override
  public boolean allMatch(Predicate<? super T> predicate) {
    return execute(() -> this.delegate.allMatch(predicate));
  }

  @Override
  public boolean noneMatch(Predicate<? super T> predicate) {
    return execute(() -> this.delegate.noneMatch(predicate));
  }

  @Override
  public Optional<T> findFirst() {
    return execute(() -> this.delegate.findFirst());
  }

  @Override
  public Optional<T> findAny() {
    return execute(() -> this.delegate.findAny());
  }

}
