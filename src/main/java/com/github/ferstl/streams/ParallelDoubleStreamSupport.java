package com.github.ferstl.streams;

import java.util.DoubleSummaryStatistics;
import java.util.OptionalDouble;
import java.util.PrimitiveIterator.OfDouble;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiConsumer;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleConsumer;
import java.util.function.DoubleFunction;
import java.util.function.DoublePredicate;
import java.util.function.DoubleToIntFunction;
import java.util.function.DoubleToLongFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.function.ObjDoubleConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;


public class ParallelDoubleStreamSupport extends AbstractParallelStreamSupport<DoubleStream> implements DoubleStream {

  ParallelDoubleStreamSupport(DoubleStream delegate, ForkJoinPool workerPool) {
    super(delegate, workerPool);
  }

  @Override
  public boolean isParallel() {
    return this.delegate.isParallel();
  }

  @Override
  public DoubleStream unordered() {
    this.delegate = this.delegate.unordered();
    return this;
  }

  @Override
  public DoubleStream onClose(Runnable closeHandler) {
    this.delegate = this.delegate.onClose(closeHandler);
    return this;
  }

  @Override
  public void close() {
    this.delegate.close();
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
  public DoubleStream sequential() {
    this.delegate = this.delegate.sequential();
    return this;
  }

  @Override
  public DoubleStream parallel() {
    this.delegate = this.delegate.parallel();
    return this;
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
