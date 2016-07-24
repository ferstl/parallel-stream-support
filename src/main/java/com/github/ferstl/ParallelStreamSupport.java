package com.github.ferstl;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
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
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import static java.util.concurrent.ForkJoinTask.adapt;

public class ParallelStreamSupport<T> implements Stream<T> {

  private Stream<T> delegate;
  private final ForkJoinPool workerPool;

  private ParallelStreamSupport(Stream<T> delegate, ForkJoinPool pool) {
    this.delegate = delegate;
    this.workerPool = pool;
  }

  public static <T> Stream<T> parallelStream(Collection<T> collection, ForkJoinPool workerPool) {
    return new ParallelStreamSupport<T>(collection.parallelStream(), workerPool);
  }

  // BaseStream methods

  @Override
  public Iterator<T> iterator() {
    return this.delegate.iterator();
  }

  @Override
  public Spliterator<T> spliterator() {
    return this.delegate.spliterator();
  }

  @Override
  public boolean isParallel() {
    return this.delegate.isParallel();
  }

  @Override
  public Stream<T> sequential() {
    this.delegate = this.delegate.sequential();
    return this;
  }

  @Override
  public Stream<T> parallel() {
    this.delegate = this.delegate.parallel();
    return this;
  }

  @Override
  public Stream<T> unordered() {
    this.delegate = this.delegate.unordered();
    return this;
  }

  @Override
  public Stream<T> onClose(Runnable closeHandler) {
    this.delegate = this.delegate.onClose(closeHandler);
    return this;
  }

  @Override
  public void close() {
    this.delegate.close();
  }

  // Stream methods

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
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public LongStream mapToLong(ToLongFunction<? super T> mapper) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public DoubleStream mapToDouble(ToDoubleFunction<? super T> mapper) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public <R> Stream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
    return new ParallelStreamSupport<>(this.delegate.flatMap(mapper), this.workerPool);
  }

  @Override
  public IntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public LongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public DoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper) {
    throw new UnsupportedOperationException("Not yet implemented");
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
    if (isParallel()) {
      ForkJoinTask<?> task = adapt(() -> this.delegate.forEach(action));
      this.workerPool.invoke(task);
    } else {
      this.delegate.forEach(action);
    }
  }

  @Override
  public void forEachOrdered(Consumer<? super T> action) {
    if (isParallel()) {
      ForkJoinTask<?> task = adapt(() -> this.delegate.forEachOrdered(action));
      this.workerPool.invoke(task);
    } else {
      this.delegate.forEach(action);
    }
  }

  @Override
  public Object[] toArray() {
    if (isParallel()) {
      ForkJoinTask<Object[]> task = adapt(() -> this.delegate.toArray());
      return this.workerPool.invoke(task);
    }

    return this.delegate.toArray();
  }

  @Override
  public <A> A[] toArray(IntFunction<A[]> generator) {
    if (isParallel()) {
      ForkJoinTask<A[]> task = adapt(() -> this.delegate.toArray(generator));
      return this.workerPool.invoke(task);
    }

    return this.delegate.toArray(generator);
  }

  @Override
  public T reduce(T identity, BinaryOperator<T> accumulator) {
    if (isParallel()) {
      ForkJoinTask<T> task = adapt(() -> this.delegate.reduce(identity, accumulator));
      return this.workerPool.invoke(task);
    }

    return this.delegate.reduce(identity, accumulator);
  }

  @Override
  public Optional<T> reduce(BinaryOperator<T> accumulator) {
    if (isParallel()) {
      ForkJoinTask<Optional<T>> task = adapt(() -> this.delegate.reduce(accumulator));
      return this.workerPool.invoke(task);
    }

    return this.delegate.reduce(accumulator);
  }

  @Override
  public <U> U reduce(U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner) {
    if (isParallel()) {
      ForkJoinTask<U> task = adapt(() -> this.delegate.reduce(identity, accumulator, combiner));
      return this.workerPool.invoke(task);
    }

    return this.delegate.reduce(identity, accumulator, combiner);
  }

  @Override
  public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
    if (isParallel()) {
      ForkJoinTask<R> task = adapt(() -> this.delegate.collect(supplier, accumulator, combiner));
      return this.workerPool.invoke(task);
    }

    return this.delegate.collect(supplier, accumulator, combiner);
  }

  @Override
  public <R, A> R collect(Collector<? super T, A, R> collector) {
    if (isParallel()) {
      ForkJoinTask<R> task = adapt(() -> this.delegate.collect(collector));
      return this.workerPool.invoke(task);
    }

    return this.delegate.collect(collector);
  }

  @Override
  public Optional<T> min(Comparator<? super T> comparator) {
    if (isParallel()) {
      ForkJoinTask<Optional<T>> task = adapt(() -> this.delegate.min(comparator));
      return this.workerPool.invoke(task);
    }

    return this.delegate.min(comparator);
  }

  @Override
  public Optional<T> max(Comparator<? super T> comparator) {
    if (isParallel()) {
      ForkJoinTask<Optional<T>> task = adapt(() -> this.delegate.max(comparator));
      return this.workerPool.invoke(task);
    }

    return this.delegate.max(comparator);
  }

  @Override
  public long count() {
    if (isParallel()) {
      ForkJoinTask<Long> task = adapt(() -> this.delegate.count());
      return this.workerPool.invoke(task);
    }

    return this.delegate.count();
  }

  @Override
  public boolean anyMatch(Predicate<? super T> predicate) {
    if (isParallel()) {
      ForkJoinTask<Boolean> task = adapt(() -> this.delegate.anyMatch(predicate));
      return this.workerPool.invoke(task);
    }

    return this.delegate.anyMatch(predicate);
  }

  @Override
  public boolean allMatch(Predicate<? super T> predicate) {
    if (isParallel()) {
      ForkJoinTask<Boolean> task = adapt(() -> this.delegate.allMatch(predicate));
      return this.workerPool.invoke(task);
    }

    return this.delegate.allMatch(predicate);
  }

  @Override
  public boolean noneMatch(Predicate<? super T> predicate) {
    if (isParallel()) {
      ForkJoinTask<Boolean> task = adapt(() -> this.delegate.noneMatch(predicate));
      return this.workerPool.invoke(task);
    }

    return this.delegate.noneMatch(predicate);
  }

  @Override
  public Optional<T> findFirst() {
    if (isParallel()) {
      ForkJoinTask<Optional<T>> task = adapt(() -> this.delegate.findFirst());
      return this.workerPool.invoke(task);
    }

    return this.delegate.findFirst();
  }

  @Override
  public Optional<T> findAny() {
    if (isParallel()) {
      ForkJoinTask<Optional<T>> task = adapt(() -> this.delegate.findAny());
      return this.workerPool.invoke(task);
    }

    return this.delegate.findAny();
  }
}

