package com.github.ferstl.streams;

import java.util.LongSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.PrimitiveIterator.OfLong;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiConsumer;
import java.util.function.LongBinaryOperator;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.LongPredicate;
import java.util.function.LongToDoubleFunction;
import java.util.function.LongToIntFunction;
import java.util.function.LongUnaryOperator;
import java.util.function.ObjLongConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;


class ParallelLongStreamSupport extends AbstractParallelStreamSupport<LongStream> implements LongStream {

  ParallelLongStreamSupport(LongStream delegate, ForkJoinPool workerPool) {
    super(delegate, workerPool);
  }

  @Override
  public boolean isParallel() {
    return this.delegate.isParallel();
  }

  @Override
  public LongStream unordered() {
    this.delegate = this.delegate.unordered();
    return this;
  }

  @Override
  public LongStream onClose(Runnable closeHandler) {
    this.delegate = this.delegate.onClose(closeHandler);
    return this;
  }

  @Override
  public void close() {
    this.delegate.close();
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
  public LongStream sequential() {
    this.delegate = this.delegate.sequential();
    return this;
  }

  @Override
  public LongStream parallel() {
    this.delegate = this.delegate.parallel();
    return this;
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
