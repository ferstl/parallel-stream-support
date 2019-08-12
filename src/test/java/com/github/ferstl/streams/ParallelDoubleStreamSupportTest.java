/*
 * Copyright (c) 2016 Stefan Ferstl <st.ferstl@gmail.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.github.ferstl.streams;

import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.OptionalDouble;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicReference;
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
import java.util.stream.DoubleStream.Builder;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;
import static java.lang.Thread.currentThread;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class ParallelDoubleStreamSupportTest extends AbstractParallelStreamSupportTest<Double, DoubleStream, ParallelDoubleStreamSupport> {

  private Stream<?> mappedDelegateMock;
  private IntStream mappedIntDelegateMock;
  private LongStream mappedLongDelegateMock;
  private DoubleStream mappedDoubleDelegateMock;
  private PrimitiveIterator.OfDouble iteratorMock;
  private Spliterator.OfDouble spliteratorMock;
  private double[] toArrayResult;
  private DoubleSummaryStatistics summaryStatistics;

  private DoubleStream delegate;
  private ParallelDoubleStreamSupport parallelDoubleStreamSupport;

  @Override
  protected ParallelDoubleStreamSupport createParallelStreamSupportMock(ForkJoinPool workerPool) {
    return new ParallelDoubleStreamSupport(mock(DoubleStream.class), workerPool);
  }

  @Before
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void init() {
    // Precondition for all tests
    assertFalse("This test must not run in a ForkJoinPool", currentThread() instanceof ForkJoinWorkerThread);

    this.mappedDelegateMock = mock(Stream.class);
    this.mappedIntDelegateMock = mock(IntStream.class);
    this.mappedLongDelegateMock = mock(LongStream.class);
    this.mappedDoubleDelegateMock = mock(DoubleStream.class);
    this.iteratorMock = mock(PrimitiveIterator.OfDouble.class);
    this.spliteratorMock = mock(Spliterator.OfDouble.class);
    this.toArrayResult = new double[0];
    this.summaryStatistics = new DoubleSummaryStatistics();

    when(this.delegateMock.map(anyObject())).thenReturn(this.mappedDoubleDelegateMock);
    when(this.delegateMock.mapToObj(anyObject())).thenReturn((Stream) this.mappedDelegateMock);
    when(this.delegateMock.mapToInt(anyObject())).thenReturn(this.mappedIntDelegateMock);
    when(this.delegateMock.mapToLong(anyObject())).thenReturn(this.mappedLongDelegateMock);
    when(this.delegateMock.flatMap(anyObject())).thenReturn(this.mappedDoubleDelegateMock);
    when(this.delegateMock.iterator()).thenReturn(this.iteratorMock);
    when(this.delegateMock.spliterator()).thenReturn(this.spliteratorMock);
    when(this.delegateMock.isParallel()).thenReturn(false);
    when(this.delegateMock.toArray()).thenReturn(this.toArrayResult);
    when(this.delegateMock.reduce(anyInt(), anyObject())).thenReturn(42.0);
    when(this.delegateMock.reduce(anyObject())).thenReturn(OptionalDouble.of(42.0));
    when(this.delegateMock.collect(anyObject(), anyObject(), anyObject())).thenReturn("collect");
    when(this.delegateMock.sum()).thenReturn(42.0);
    when(this.delegateMock.min()).thenReturn(OptionalDouble.of(42.0));
    when(this.delegateMock.max()).thenReturn(OptionalDouble.of(42.0));
    when(this.delegateMock.count()).thenReturn(42L);
    when(this.delegateMock.average()).thenReturn(OptionalDouble.of(42.0));
    when(this.delegateMock.summaryStatistics()).thenReturn(this.summaryStatistics);
    when(this.delegateMock.anyMatch(anyObject())).thenReturn(true);
    when(this.delegateMock.allMatch(anyObject())).thenReturn(true);
    when(this.delegateMock.noneMatch(anyObject())).thenReturn(true);
    when(this.delegateMock.findFirst()).thenReturn(OptionalDouble.of(42.0));
    when(this.delegateMock.findAny()).thenReturn(OptionalDouble.of(42.0));
    when(this.delegateMock.boxed()).thenReturn((Stream) this.mappedDelegateMock);

    this.delegate = DoubleStream.of(1.0).parallel();
    this.parallelDoubleStreamSupport = new ParallelDoubleStreamSupport(this.delegate, this.workerPool);
  }


  @Test
  public void parallelStreamWithArray() {
    DoubleStream stream = ParallelDoubleStreamSupport.parallelStream(new double[]{42.0}, this.workerPool);

    assertThat(stream, instanceOf(ParallelDoubleStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(OptionalDouble.of(42.0), stream.findAny());
  }

  @Test(expected = NullPointerException.class)
  public void parallelStreamWithNullArray() {
    ParallelDoubleStreamSupport.parallelStream((double[]) null, this.workerPool);
  }

  @Test
  public void parallelStreamSupportWithSpliterator() {
    Spliterator.OfDouble spliterator = DoubleStream.of(42.0).spliterator();
    DoubleStream stream = ParallelDoubleStreamSupport.parallelStream(spliterator, this.workerPool);

    assertThat(stream, instanceOf(ParallelDoubleStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(OptionalDouble.of(42.0), stream.findAny());
  }

  @Test(expected = NullPointerException.class)
  public void parallelStreamSupportWithNullSpliterator() {
    ParallelDoubleStreamSupport.parallelStream((Spliterator.OfDouble) null, this.workerPool);
  }

  @Test
  public void parallelStreamSupportWithSpliteratorSupplier() {
    Supplier<Spliterator.OfDouble> supplier = () -> DoubleStream.of(42.0).spliterator();
    DoubleStream stream = ParallelDoubleStreamSupport.parallelStream(supplier, 0, this.workerPool);

    assertThat(stream, instanceOf(ParallelDoubleStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(OptionalDouble.of(42.0), stream.findAny());
  }

  @Test(expected = NullPointerException.class)
  public void parallelStreamSupportWithNullSpliteratorSupplier() {
    ParallelDoubleStreamSupport.parallelStream(null, 0, this.workerPool);
  }

  @Test
  public void parallelStreamWithBuilder() {
    Builder builder = DoubleStream.builder();
    builder.accept(42.0);
    DoubleStream stream = ParallelDoubleStreamSupport.parallelStream(builder, this.workerPool);

    assertThat(stream, instanceOf(ParallelDoubleStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(OptionalDouble.of(42.0), stream.findAny());
  }

  @Test(expected = NullPointerException.class)
  public void parallelStreamWithNullBuilder() {
    ParallelDoubleStreamSupport.parallelStream((Builder) null, this.workerPool);
  }

  @Test
  public void iterate() {
    DoubleUnaryOperator operator = a -> a;
    DoubleStream stream = ParallelDoubleStreamSupport.iterate(42.0, operator, this.workerPool);

    assertThat(stream, instanceOf(ParallelDoubleStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(OptionalDouble.of(42.0), stream.findAny());
  }

  @Test(expected = NullPointerException.class)
  public void iterateWithNullOperator() {
    ParallelDoubleStreamSupport.iterate(42.0, null, this.workerPool);
  }

  @Test
  public void generate() {
    DoubleSupplier supplier = () -> 42.0;
    DoubleStream stream = ParallelDoubleStreamSupport.generate(supplier, this.workerPool);

    assertThat(stream, instanceOf(ParallelDoubleStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(OptionalDouble.of(42.0), stream.findAny());
  }

  @Test(expected = NullPointerException.class)
  public void generateWithNullSupplier() {
    ParallelDoubleStreamSupport.generate(null, this.workerPool);
  }

  @Test
  public void concat() {
    DoubleStream a = DoubleStream.of(42.0);
    DoubleStream b = DoubleStream.of(43);
    DoubleStream stream = ParallelDoubleStreamSupport.concat(a, b, this.workerPool);

    assertThat(stream, instanceOf(ParallelDoubleStreamSupport.class));
    assertTrue(stream.isParallel());
    assertArrayEquals(stream.toArray(), new double[]{42.0, 43.0}, 0.000001);
  }

  @Test(expected = NullPointerException.class)
  public void concatWithNullStreamA() {
    ParallelDoubleStreamSupport.concat(null, DoubleStream.of(42.0), this.workerPool);
  }

  @Test(expected = NullPointerException.class)
  public void concatWithNullStreamB() {
    ParallelDoubleStreamSupport.concat(DoubleStream.of(42.0), null, this.workerPool);
  }

  @Test
  public void filter() {
    DoublePredicate p = d -> true;
    DoubleStream stream = this.parallelStreamSupportMock.filter(p);

    verify(this.delegateMock).filter(p);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void map() {
    DoubleUnaryOperator f = d -> 42;
    DoubleStream stream = this.parallelStreamSupportMock.map(f);

    verify(this.delegateMock).map(f);
    assertThat(stream, instanceOf(ParallelDoubleStreamSupport.class));
    assertSame(ParallelDoubleStreamSupport.class.cast(stream).delegate, this.mappedDoubleDelegateMock);
    assertSame(ParallelDoubleStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  public void mapToObj() {
    DoubleFunction<String> f = d -> "x";
    Stream<String> stream = this.parallelStreamSupportMock.mapToObj(f);

    verify(this.delegateMock).mapToObj(f);
    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertSame(ParallelStreamSupport.class.cast(stream).delegate, this.mappedDelegateMock);
    assertSame(ParallelStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  public void mapToInt() {
    DoubleToIntFunction f = d -> 1;
    IntStream stream = this.parallelStreamSupportMock.mapToInt(f);

    verify(this.delegateMock).mapToInt(f);
    assertThat(stream, instanceOf(ParallelIntStreamSupport.class));
    assertSame(ParallelIntStreamSupport.class.cast(stream).delegate, this.mappedIntDelegateMock);
    assertSame(ParallelIntStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  public void mapToLong() {
    DoubleToLongFunction f = d -> 1L;
    LongStream stream = this.parallelStreamSupportMock.mapToLong(f);

    verify(this.delegateMock).mapToLong(f);
    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertSame(ParallelLongStreamSupport.class.cast(stream).delegate, this.mappedLongDelegateMock);
    assertSame(ParallelLongStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  public void flatMap() {
    DoubleFunction<DoubleStream> f = d -> DoubleStream.of(1.0);
    DoubleStream stream = this.parallelStreamSupportMock.flatMap(f);

    verify(this.delegateMock).flatMap(f);
    assertThat(stream, instanceOf(ParallelDoubleStreamSupport.class));
    assertSame(ParallelDoubleStreamSupport.class.cast(stream).delegate, this.mappedDoubleDelegateMock);
  }

  @Test
  public void distinct() {
    DoubleStream stream = this.parallelStreamSupportMock.distinct();

    verify(this.delegateMock).distinct();
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void sorted() {
    DoubleStream stream = this.parallelStreamSupportMock.sorted();

    verify(this.delegateMock).sorted();
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void peek() {
    DoubleConsumer c = d -> {};
    DoubleStream stream = this.parallelStreamSupportMock.peek(c);

    verify(this.delegateMock).peek(c);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void limit() {
    DoubleStream stream = this.parallelStreamSupportMock.limit(5);

    verify(this.delegateMock).limit(5);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void skip() {
    DoubleStream stream = this.parallelStreamSupportMock.skip(5);

    verify(this.delegateMock).skip(5);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void forEach() {
    DoubleConsumer c = d -> {};
    this.parallelStreamSupportMock.forEach(c);

    verify(this.delegateMock).forEach(c);
  }

  @Test
  public void forEachSequential() {
    this.parallelDoubleStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport.forEach(d -> threadRef.set(currentThread()));

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void forEachParallel() {
    this.parallelDoubleStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport.forEach(d -> threadRef.set(currentThread()));

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void forEachOrdered() {
    DoubleConsumer c = d -> {};
    this.parallelStreamSupportMock.forEachOrdered(c);

    verify(this.delegateMock).forEachOrdered(c);
  }

  @Test
  public void forEachOrderedSequential() {
    this.parallelDoubleStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport.forEachOrdered(d -> threadRef.set(currentThread()));

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void forEachOrderedParallel() {
    this.parallelDoubleStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport.forEachOrdered(d -> threadRef.set(currentThread()));

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void toArray() {
    double[] array = this.parallelStreamSupportMock.toArray();

    verify(this.delegateMock).toArray();
    assertSame(this.toArrayResult, array);
  }

  @Test
  public void toArraySequential() {
    this.parallelDoubleStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .toArray();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void toArrayParallel() {
    this.parallelDoubleStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .toArray();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void reduceWithIdentityAndAccumulator() {
    DoubleBinaryOperator accumulator = (a, b) -> b;
    double result = this.parallelStreamSupportMock.reduce(0, accumulator);

    verify(this.delegateMock).reduce(0, accumulator);
    assertEquals(42.0, result, 0.000001);
  }

  @Test
  public void reduceWithIdentityAndAccumulatorSequential() {
    this.parallelDoubleStreamSupport.sequential();
    DoubleBinaryOperator accumulator = (a, b) -> b;
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .reduce(0, accumulator);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void reduceWithIdentityAndAccumulatorParallel() {
    this.parallelDoubleStreamSupport.parallel();
    DoubleBinaryOperator accumulator = (a, b) -> b;
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .reduce(0, accumulator);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void reduceWithAccumulator() {
    DoubleBinaryOperator accumulator = (a, b) -> b;
    OptionalDouble result = this.parallelStreamSupportMock.reduce(accumulator);

    verify(this.delegateMock).reduce(accumulator);
    assertEquals(OptionalDouble.of(42), result);
  }

  @Test
  public void reduceWithAccumulatorSequential() {
    this.parallelDoubleStreamSupport.sequential();
    DoubleBinaryOperator accumulator = (a, b) -> b;
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .reduce(accumulator);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void reduceWithAccumulatorParallel() {
    this.parallelDoubleStreamSupport.parallel();
    DoubleBinaryOperator accumulator = (a, b) -> b;
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .reduce(accumulator);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void collectWithSupplierAndAccumulatorAndCombiner() {
    Supplier<String> supplier = () -> "x";
    ObjDoubleConsumer<String> accumulator = (a, b) -> {};
    BiConsumer<String, String> combiner = (a, b) -> {};

    String result = this.parallelStreamSupportMock.collect(supplier, accumulator, combiner);

    verify(this.delegateMock).collect(supplier, accumulator, combiner);
    assertEquals("collect", result);
  }

  @Test
  public void collectWithSupplierAndAccumulatorAndCombinerSequential() {
    this.parallelDoubleStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void collectWithSupplierAndAccumulatorAndCombinerParallel() {
    this.parallelDoubleStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void sum() {
    double result = this.parallelStreamSupportMock.sum();

    verify(this.delegateMock).sum();
    assertEquals(42.0, result, 0.000001);
  }

  @Test
  public void sumSequential() {
    this.parallelDoubleStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .sum();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void sumParallel() {
    this.parallelDoubleStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .sum();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void min() {
    OptionalDouble result = this.parallelStreamSupportMock.min();

    verify(this.delegateMock).min();
    assertEquals(OptionalDouble.of(42), result);
  }

  @Test
  public void minSequential() {
    this.parallelDoubleStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .min();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void minParallel() {
    this.parallelDoubleStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .min();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void max() {
    OptionalDouble result = this.parallelStreamSupportMock.max();

    verify(this.delegateMock).max();
    assertEquals(OptionalDouble.of(42), result);
  }

  @Test
  public void maxSequential() {
    this.parallelDoubleStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .max();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void maxParallel() {
    this.parallelDoubleStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .max();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void count() {
    long count = this.parallelStreamSupportMock.count();

    verify(this.delegateMock).count();
    assertEquals(42L, count);
  }

  @Test
  public void countSequential() {
    this.parallelDoubleStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .filter(d -> {
          // Don't use peek() in combination with count(). See Javadoc.
          threadRef.set(currentThread());
          return true;
        }).count();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void countParallel() {
    this.parallelDoubleStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .filter(d -> {
          // Don't use peek() in combination with count(). See Javadoc.
          threadRef.set(currentThread());
          return true;
        }).count();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void average() {
    OptionalDouble result = this.parallelStreamSupportMock.average();

    verify(this.delegateMock).average();
    assertEquals(OptionalDouble.of(42.0), result);
  }

  @Test
  public void averageSequential() {
    this.parallelDoubleStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .average();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void averageParallel() {
    this.parallelDoubleStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .average();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void summaryStatistics() {
    DoubleSummaryStatistics result = this.parallelStreamSupportMock.summaryStatistics();

    verify(this.delegateMock).summaryStatistics();
    assertEquals(this.summaryStatistics, result);
  }

  @Test
  public void summaryStatisticsSequential() {
    this.parallelDoubleStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .summaryStatistics();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void summaryStatisticsParallel() {
    this.parallelDoubleStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .summaryStatistics();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void anyMatch() {
    DoublePredicate p = d -> true;

    boolean result = this.parallelStreamSupportMock.anyMatch(p);

    verify(this.delegateMock).anyMatch(p);
    assertTrue(result);
  }

  @Test
  public void anyMatchSequential() {
    this.parallelDoubleStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .anyMatch(d -> true);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void anyMatchParallel() {
    this.parallelDoubleStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .anyMatch(d -> true);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void allMatch() {
    DoublePredicate p = d -> true;

    boolean result = this.parallelStreamSupportMock.allMatch(p);

    verify(this.delegateMock).allMatch(p);
    assertTrue(result);
  }

  @Test
  public void allMatchSequential() {
    this.parallelDoubleStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .allMatch(d -> true);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void allMatchParallel() {
    this.parallelDoubleStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .allMatch(d -> true);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void noneMatch() {
    DoublePredicate p = d -> true;

    boolean result = this.parallelStreamSupportMock.noneMatch(p);

    verify(this.delegateMock).noneMatch(p);
    assertTrue(result);
  }

  @Test
  public void noneMatchSequential() {
    this.parallelDoubleStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .noneMatch(d -> true);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void noneMatchParallel() {
    this.parallelDoubleStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .noneMatch(d -> true);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void findFirst() {
    OptionalDouble result = this.parallelStreamSupportMock.findFirst();

    verify(this.delegateMock).findFirst();
    assertEquals(OptionalDouble.of(42), result);
  }

  @Test
  public void findFirstSequential() {
    this.parallelDoubleStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .findFirst();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void findFirstParallel() {
    this.parallelDoubleStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .findFirst();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void findAny() {
    OptionalDouble result = this.parallelStreamSupportMock.findAny();

    verify(this.delegateMock).findAny();
    assertEquals(OptionalDouble.of(42), result);
  }

  @Test
  public void findAnytSequential() {
    this.parallelDoubleStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .findAny();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void findAnyParallel() {
    this.parallelDoubleStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelDoubleStreamSupport
        .peek(d -> threadRef.set(currentThread()))
        .findAny();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void boxed() {
    Stream<Double> stream = this.parallelStreamSupportMock.boxed();

    verify(this.delegateMock).boxed();
    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertSame(this.mappedDelegateMock, ParallelStreamSupport.class.cast(stream).delegate);
    assertSame(this.workerPool, ParallelStreamSupport.class.cast(stream).workerPool);
  }

  @Override
  @Test
  public void iterator() {
    PrimitiveIterator.OfDouble iterator = this.parallelStreamSupportMock.iterator();

    verify(this.delegateMock).iterator();
    assertSame(this.iteratorMock, iterator);
  }

  @Override
  @Test
  public void spliterator() {
    Spliterator.OfDouble spliterator = this.parallelStreamSupportMock.spliterator();

    verify(this.delegateMock).spliterator();
    assertSame(this.spliteratorMock, spliterator);
  }
}
