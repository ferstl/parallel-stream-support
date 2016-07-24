package com.github.ferstl;

import java.util.IntSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.PrimitiveIterator.OfInt;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiConsumer;
import java.util.function.IntBinaryOperator;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.IntToDoubleFunction;
import java.util.function.IntToLongFunction;
import java.util.function.IntUnaryOperator;
import java.util.function.ObjIntConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;


public class ParallelIntStreamSupport implements IntStream {

  private IntStream delegate;
  private final ForkJoinPool workerPool;

  ParallelIntStreamSupport(IntStream delegate, ForkJoinPool workerPool) {
    this.delegate = delegate;
    this.workerPool = workerPool;
  }

  @Override
  public IntStream filter(IntPredicate predicate) {
    this.delegate = this.delegate.filter(predicate);
    return this;
  }

  @Override
  public boolean isParallel() {
    return this.delegate.isParallel();
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
  public IntStream unordered() {
    this.delegate = this.delegate.unordered();
    return this;
  }

  @Override
  public LongStream mapToLong(IntToLongFunction mapper) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public IntStream onClose(Runnable closeHandler) {
    this.delegate = this.delegate.onClose(closeHandler);
    return this;
  }

  @Override
  public DoubleStream mapToDouble(IntToDoubleFunction mapper) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void close() {
    this.delegate.close();
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
    throw new UnsupportedOperationException("Not yet implemented");
    // this.delegate.forEach(action);
  }

  @Override
  public void forEachOrdered(IntConsumer action) {
    throw new UnsupportedOperationException("Not yet implemented");
    // this.delegate.forEachOrdered(action);
  }

  @Override
  public int[] toArray() {
    throw new UnsupportedOperationException("Not yet implemented");
    // return this.delegate.toArray();
  }

  @Override
  public int reduce(int identity, IntBinaryOperator op) {
    throw new UnsupportedOperationException("Not yet implemented");
    // return this.delegate.reduce(identity, op);
  }

  @Override
  public OptionalInt reduce(IntBinaryOperator op) {
    throw new UnsupportedOperationException("Not yet implemented");
    // return this.delegate.reduce(op);
  }

  @Override
  public <R> R collect(Supplier<R> supplier, ObjIntConsumer<R> accumulator, BiConsumer<R, R> combiner) {
    throw new UnsupportedOperationException("Not yet implemented");
    // return this.delegate.collect(supplier, accumulator, combiner);
  }

  @Override
  public int sum() {
    throw new UnsupportedOperationException("Not yet implemented");
    // return this.delegate.sum();
  }

  @Override
  public OptionalInt min() {
    throw new UnsupportedOperationException("Not yet implemented");
    // return this.delegate.min();
  }

  @Override
  public OptionalInt max() {
    throw new UnsupportedOperationException("Not yet implemented");
    // return this.delegate.max();
  }

  @Override
  public long count() {
    throw new UnsupportedOperationException("Not yet implemented");
    // return this.delegate.count();
  }

  @Override
  public OptionalDouble average() {
    throw new UnsupportedOperationException("Not yet implemented");
    // return this.delegate.average();
  }

  @Override
  public IntSummaryStatistics summaryStatistics() {
    throw new UnsupportedOperationException("Not yet implemented");
    // return this.delegate.summaryStatistics();
  }

  @Override
  public boolean anyMatch(IntPredicate predicate) {
    throw new UnsupportedOperationException("Not yet implemented");
    // return this.delegate.anyMatch(predicate);
  }

  @Override
  public boolean allMatch(IntPredicate predicate) {
    throw new UnsupportedOperationException("Not yet implemented");
    // return this.delegate.allMatch(predicate);
  }

  @Override
  public boolean noneMatch(IntPredicate predicate) {
    throw new UnsupportedOperationException("Not yet implemented");
    // return this.delegate.noneMatch(predicate);
  }

  @Override
  public OptionalInt findFirst() {
    throw new UnsupportedOperationException("Not yet implemented");
    // return this.delegate.findFirst();
  }

  @Override
  public OptionalInt findAny() {
    throw new UnsupportedOperationException("Not yet implemented");
    // return this.delegate.findAny();
  }

  @Override
  public LongStream asLongStream() {
    throw new UnsupportedOperationException("Not yet implemented");
    // return this.delegate.asLongStream();
  }

  @Override
  public DoubleStream asDoubleStream() {
    throw new UnsupportedOperationException("Not yet implemented");
    // return this.delegate.asDoubleStream();
  }

  @Override
  public Stream<Integer> boxed() {
    throw new UnsupportedOperationException("Not yet implemented");
    // return this.delegate.boxed();
  }

  @Override
  public IntStream sequential() {
    throw new UnsupportedOperationException("Not yet implemented");
    // return this.delegate.sequential();
  }

  @Override
  public IntStream parallel() {
    throw new UnsupportedOperationException("Not yet implemented");
    // return this.delegate.parallel();
  }

  @Override
  public OfInt iterator() {
    throw new UnsupportedOperationException("Not yet implemented");
    // return this.delegate.iterator();
  }

  @Override
  public java.util.Spliterator.OfInt spliterator() {
    throw new UnsupportedOperationException("Not yet implemented");
    // return this.delegate.spliterator();
  }


}
