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
import java.util.LongSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicReference;
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
import java.util.stream.LongStream.Builder;
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
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class ParallelLongStreamSupportTest extends AbstractParallelStreamSupportTest<Long, LongStream, ParallelLongStreamSupport> {

  private Stream<?> mappedDelegateMock;
  private IntStream mappedIntDelegateMock;
  private LongStream mappedLongDelegateMock;
  private DoubleStream mappedDoubleDelegateMock;
  private PrimitiveIterator.OfLong iteratorMock;
  private Spliterator.OfLong spliteratorMock;
  private long[] toArrayResult;
  private LongSummaryStatistics summaryStatistics;

  private LongStream delegate;
  private ParallelLongStreamSupport parallelLongStreamSupport;

  @Override
  protected ParallelLongStreamSupport createParallelStreamSupportMock(ForkJoinPool workerPool) {
    return new ParallelLongStreamSupport(mock(LongStream.class), workerPool);
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
    this.iteratorMock = mock(PrimitiveIterator.OfLong.class);
    this.spliteratorMock = mock(Spliterator.OfLong.class);
    this.toArrayResult = new long[0];
    this.summaryStatistics = new LongSummaryStatistics();

    when(this.delegateMock.map(anyObject())).thenReturn(this.mappedLongDelegateMock);
    when(this.delegateMock.mapToObj(anyObject())).thenReturn((Stream) this.mappedDelegateMock);
    when(this.delegateMock.mapToInt(anyObject())).thenReturn(this.mappedIntDelegateMock);
    when(this.delegateMock.mapToDouble(anyObject())).thenReturn(this.mappedDoubleDelegateMock);
    when(this.delegateMock.flatMap(anyObject())).thenReturn(this.mappedLongDelegateMock);
    when(this.delegateMock.iterator()).thenReturn(this.iteratorMock);
    when(this.delegateMock.spliterator()).thenReturn(this.spliteratorMock);
    when(this.delegateMock.isParallel()).thenReturn(false);
    when(this.delegateMock.toArray()).thenReturn(this.toArrayResult);
    when(this.delegateMock.reduce(anyLong(), anyObject())).thenReturn(42L);
    when(this.delegateMock.reduce(anyObject())).thenReturn(OptionalLong.of(42));
    when(this.delegateMock.collect(anyObject(), anyObject(), anyObject())).thenReturn("collect");
    when(this.delegateMock.sum()).thenReturn(42L);
    when(this.delegateMock.min()).thenReturn(OptionalLong.of(42));
    when(this.delegateMock.max()).thenReturn(OptionalLong.of(42));
    when(this.delegateMock.count()).thenReturn(42L);
    when(this.delegateMock.average()).thenReturn(OptionalDouble.of(42.0));
    when(this.delegateMock.summaryStatistics()).thenReturn(this.summaryStatistics);
    when(this.delegateMock.anyMatch(anyObject())).thenReturn(true);
    when(this.delegateMock.allMatch(anyObject())).thenReturn(true);
    when(this.delegateMock.noneMatch(anyObject())).thenReturn(true);
    when(this.delegateMock.findFirst()).thenReturn(OptionalLong.of(42));
    when(this.delegateMock.findAny()).thenReturn(OptionalLong.of(42));
    when(this.delegateMock.asDoubleStream()).thenReturn(this.mappedDoubleDelegateMock);
    when(this.delegateMock.boxed()).thenReturn((Stream) this.mappedDelegateMock);

    this.delegate = LongStream.of(1L).parallel();
    this.parallelLongStreamSupport = new ParallelLongStreamSupport(this.delegate, this.workerPool);
  }

  @Test
  public void parallelStreamWithArray() {
    LongStream stream = ParallelLongStreamSupport.parallelStream(new long[]{42}, this.workerPool);

    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(OptionalLong.of(42), stream.findAny());
  }

  @Test(expected = NullPointerException.class)
  public void parallelStreamWithNullArray() {
    ParallelLongStreamSupport.parallelStream((long[]) null, this.workerPool);
  }

  @Test
  public void parallelStreamSupportWithSpliterator() {
    Spliterator.OfLong spliterator = LongStream.of(42).spliterator();
    LongStream stream = ParallelLongStreamSupport.parallelStream(spliterator, this.workerPool);

    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(OptionalLong.of(42), stream.findAny());
  }

  @Test(expected = NullPointerException.class)
  public void parallelStreamSupportWithNullSpliterator() {
    ParallelLongStreamSupport.parallelStream((Spliterator.OfLong) null, this.workerPool);
  }

  @Test
  public void parallelStreamSupportWithSpliteratorSupplier() {
    Supplier<Spliterator.OfLong> supplier = () -> LongStream.of(42).spliterator();
    LongStream stream = ParallelLongStreamSupport.parallelStream(supplier, 0, this.workerPool);

    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(OptionalLong.of(42), stream.findAny());
  }

  @Test(expected = NullPointerException.class)
  public void parallelStreamSupportWithNullSpliteratorSupplier() {
    ParallelLongStreamSupport.parallelStream(null, 0, this.workerPool);
  }

  @Test
  public void parallelStreamWithBuilder() {
    Builder builder = LongStream.builder();
    builder.accept(42);
    LongStream stream = ParallelLongStreamSupport.parallelStream(builder, this.workerPool);

    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(OptionalLong.of(42), stream.findAny());
  }

  @Test(expected = NullPointerException.class)
  public void parallelStreamWithNullBuilder() {
    ParallelLongStreamSupport.parallelStream((Builder) null, this.workerPool);
  }

  @Test
  public void iterate() {
    LongUnaryOperator operator = a -> a;
    LongStream stream = ParallelLongStreamSupport.iterate(42, operator, this.workerPool);

    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(OptionalLong.of(42), stream.findAny());
  }

  @Test(expected = NullPointerException.class)
  public void iterateWithNullOperator() {
    ParallelLongStreamSupport.iterate(42, null, this.workerPool);
  }

  @Test
  public void generate() {
    LongSupplier supplier = () -> 42;
    LongStream stream = ParallelLongStreamSupport.generate(supplier, this.workerPool);

    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(OptionalLong.of(42), stream.findAny());
  }

  @Test(expected = NullPointerException.class)
  public void generateWithNullSupplier() {
    ParallelLongStreamSupport.generate(null, this.workerPool);
  }

  @Test
  public void range() {
    LongStream stream = ParallelLongStreamSupport.range(0, 5, this.workerPool);

    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertTrue(stream.isParallel());
    assertArrayEquals(stream.toArray(), new long[]{0, 1, 2, 3, 4});
  }

  @Test
  public void rangeClosed() {
    LongStream stream = ParallelLongStreamSupport.rangeClosed(0, 5, this.workerPool);

    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertTrue(stream.isParallel());
    assertArrayEquals(stream.toArray(), new long[]{0, 1, 2, 3, 4, 5});
  }

  @Test
  public void concat() {
    LongStream a = LongStream.of(42);
    LongStream b = LongStream.of(43);
    LongStream stream = ParallelLongStreamSupport.concat(a, b, this.workerPool);

    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertTrue(stream.isParallel());
    assertArrayEquals(stream.toArray(), new long[]{42, 43});
  }

  @Test(expected = NullPointerException.class)
  public void concatWithNullStreamA() {
    ParallelLongStreamSupport.concat(null, LongStream.of(42), this.workerPool);
  }

  @Test(expected = NullPointerException.class)
  public void concatWithNullStreamB() {
    ParallelLongStreamSupport.concat(LongStream.of(42), null, this.workerPool);
  }

  @Test
  public void filter() {
    LongPredicate p = i -> true;
    LongStream stream = this.parallelStreamSupportMock.filter(p);

    verify(this.delegateMock).filter(p);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void map() {
    LongUnaryOperator f = i -> 42;
    LongStream stream = this.parallelStreamSupportMock.map(f);

    verify(this.delegateMock).map(f);
    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertSame(ParallelLongStreamSupport.class.cast(stream).delegate, this.mappedLongDelegateMock);
    assertSame(ParallelLongStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  public void mapToObj() {
    LongFunction<String> f = i -> "x";
    Stream<String> stream = this.parallelStreamSupportMock.mapToObj(f);

    verify(this.delegateMock).mapToObj(f);
    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertSame(ParallelStreamSupport.class.cast(stream).delegate, this.mappedDelegateMock);
    assertSame(ParallelStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  public void mapToInt() {
    LongToIntFunction f = i -> 1;
    IntStream stream = this.parallelStreamSupportMock.mapToInt(f);

    verify(this.delegateMock).mapToInt(f);
    assertThat(stream, instanceOf(ParallelIntStreamSupport.class));
    assertSame(ParallelIntStreamSupport.class.cast(stream).delegate, this.mappedIntDelegateMock);
    assertSame(ParallelIntStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  public void mapToDouble() {
    LongToDoubleFunction f = i -> 1.0;
    DoubleStream stream = this.parallelStreamSupportMock.mapToDouble(f);

    verify(this.delegateMock).mapToDouble(f);
    assertThat(stream, instanceOf(ParallelDoubleStreamSupport.class));
    assertSame(ParallelDoubleStreamSupport.class.cast(stream).delegate, this.mappedDoubleDelegateMock);
    assertSame(ParallelDoubleStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  public void flatMap() {
    LongFunction<LongStream> f = i -> LongStream.of(1);
    LongStream stream = this.parallelStreamSupportMock.flatMap(f);

    verify(this.delegateMock).flatMap(f);
    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertSame(ParallelLongStreamSupport.class.cast(stream).delegate, this.mappedLongDelegateMock);
  }

  @Test
  public void distinct() {
    LongStream stream = this.parallelStreamSupportMock.distinct();

    verify(this.delegateMock).distinct();
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void sorted() {
    LongStream stream = this.parallelStreamSupportMock.sorted();

    verify(this.delegateMock).sorted();
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void peek() {
    LongConsumer c = i -> {};
    LongStream stream = this.parallelStreamSupportMock.peek(c);

    verify(this.delegateMock).peek(c);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void limit() {
    LongStream stream = this.parallelStreamSupportMock.limit(5);

    verify(this.delegateMock).limit(5);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void skip() {
    LongStream stream = this.parallelStreamSupportMock.skip(5);

    verify(this.delegateMock).skip(5);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void forEach() {
    LongConsumer c = i -> {};
    this.parallelStreamSupportMock.forEach(c);

    verify(this.delegateMock).forEach(c);
  }

  @Test
  public void forEachSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport.forEach(i -> threadRef.set(currentThread()));

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void forEachParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport.forEach(i -> threadRef.set(currentThread()));

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void forEachOrdered() {
    LongConsumer c = i -> {};
    this.parallelStreamSupportMock.forEachOrdered(c);

    verify(this.delegateMock).forEachOrdered(c);
  }

  @Test
  public void forEachOrderedSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport.forEachOrdered(i -> threadRef.set(currentThread()));

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void forEachOrderedParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport.forEachOrdered(i -> threadRef.set(currentThread()));

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void toArray() {
    long[] array = this.parallelStreamSupportMock.toArray();

    verify(this.delegateMock).toArray();
    assertSame(this.toArrayResult, array);
  }

  @Test
  public void toArraySequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .toArray();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void toArrayParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .toArray();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void reduceWithIdentityAndAccumulator() {
    LongBinaryOperator accumulator = (a, b) -> b;
    long result = this.parallelStreamSupportMock.reduce(0, accumulator);

    verify(this.delegateMock).reduce(0, accumulator);
    assertEquals(42, result);
  }

  @Test
  public void reduceWithIdentityAndAccumulatorSequential() {
    this.parallelLongStreamSupport.sequential();
    LongBinaryOperator accumulator = (a, b) -> b;
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .reduce(0, accumulator);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void reduceWithIdentityAndAccumulatorParallel() {
    this.parallelLongStreamSupport.parallel();
    LongBinaryOperator accumulator = (a, b) -> b;
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .reduce(0, accumulator);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void reduceWithAccumulator() {
    LongBinaryOperator accumulator = (a, b) -> b;
    OptionalLong result = this.parallelStreamSupportMock.reduce(accumulator);

    verify(this.delegateMock).reduce(accumulator);
    assertEquals(OptionalLong.of(42), result);
  }

  @Test
  public void reduceWithAccumulatorSequential() {
    this.parallelLongStreamSupport.sequential();
    LongBinaryOperator accumulator = (a, b) -> b;
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .reduce(accumulator);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void reduceWithAccumulatorParallel() {
    this.parallelLongStreamSupport.parallel();
    LongBinaryOperator accumulator = (a, b) -> b;
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .reduce(accumulator);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void collectWithSupplierAndAccumulatorAndCombiner() {
    Supplier<String> supplier = () -> "x";
    ObjLongConsumer<String> accumulator = (a, b) -> {};
    BiConsumer<String, String> combiner = (a, b) -> {};

    String result = this.parallelStreamSupportMock.collect(supplier, accumulator, combiner);

    verify(this.delegateMock).collect(supplier, accumulator, combiner);
    assertEquals("collect", result);
  }

  @Test
  public void collectWithSupplierAndAccumulatorAndCombinerSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void collectWithSupplierAndAccumulatorAndCombinerParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void sum() {
    long result = this.parallelStreamSupportMock.sum();

    verify(this.delegateMock).sum();
    assertEquals(42, result);
  }

  @Test
  public void sumSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .sum();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void sumParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .sum();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void min() {
    OptionalLong result = this.parallelStreamSupportMock.min();

    verify(this.delegateMock).min();
    assertEquals(OptionalLong.of(42), result);
  }

  @Test
  public void minSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .min();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void minParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .min();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void max() {
    OptionalLong result = this.parallelStreamSupportMock.max();

    verify(this.delegateMock).max();
    assertEquals(OptionalLong.of(42), result);
  }

  @Test
  public void maxSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .max();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void maxParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .max();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void count() {
    long count = this.parallelStreamSupportMock.count();

    verify(this.delegateMock).count();
    assertEquals(42, count);
  }

  @Test
  public void countSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        // Don't use peek() in combination with count(). See Javadoc.
        .filter(i -> {
          threadRef.set(currentThread());
          return true;
        }).count();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void countParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        // Don't use peek() in combination with count(). See Javadoc.
        .filter(i -> {
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
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .average();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void averageParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .average();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void summaryStatistics() {
    LongSummaryStatistics result = this.parallelStreamSupportMock.summaryStatistics();

    verify(this.delegateMock).summaryStatistics();
    assertEquals(this.summaryStatistics, result);
  }

  @Test
  public void summaryStatisticsSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .summaryStatistics();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void summaryStatisticsParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .summaryStatistics();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void anyMatch() {
    LongPredicate p = i -> true;

    boolean result = this.parallelStreamSupportMock.anyMatch(p);

    verify(this.delegateMock).anyMatch(p);
    assertTrue(result);
  }

  @Test
  public void anyMatchSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .anyMatch(i -> true);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void anyMatchParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .anyMatch(i -> true);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void allMatch() {
    LongPredicate p = i -> true;

    boolean result = this.parallelStreamSupportMock.allMatch(p);

    verify(this.delegateMock).allMatch(p);
    assertTrue(result);
  }

  @Test
  public void allMatchSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .allMatch(i -> true);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void allMatchParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .allMatch(i -> true);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void noneMatch() {
    LongPredicate p = i -> true;

    boolean result = this.parallelStreamSupportMock.noneMatch(p);

    verify(this.delegateMock).noneMatch(p);
    assertTrue(result);
  }

  @Test
  public void noneMatchSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .noneMatch(i -> true);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void noneMatchParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .noneMatch(i -> true);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void findFirst() {
    OptionalLong result = this.parallelStreamSupportMock.findFirst();

    verify(this.delegateMock).findFirst();
    assertEquals(OptionalLong.of(42), result);
  }

  @Test
  public void findFirstSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .findFirst();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void findFirstParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .findFirst();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void findAny() {
    OptionalLong result = this.parallelStreamSupportMock.findAny();

    verify(this.delegateMock).findAny();
    assertEquals(OptionalLong.of(42), result);
  }

  @Test
  public void findAnytSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .findAny();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void findAnyParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .findAny();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void asDoubleStream() {
    DoubleStream stream = this.parallelStreamSupportMock.asDoubleStream();

    verify(this.delegateMock).asDoubleStream();
    assertThat(stream, instanceOf(ParallelDoubleStreamSupport.class));
    assertSame(this.mappedDoubleDelegateMock, ParallelDoubleStreamSupport.class.cast(stream).delegate);
    assertSame(this.workerPool, ParallelDoubleStreamSupport.class.cast(stream).workerPool);
  }

  @Test
  public void boxed() {
    Stream<Long> stream = this.parallelStreamSupportMock.boxed();

    verify(this.delegateMock).boxed();
    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertSame(this.mappedDelegateMock, ParallelStreamSupport.class.cast(stream).delegate);
    assertSame(this.workerPool, ParallelStreamSupport.class.cast(stream).workerPool);
  }

  @Override
  @Test
  public void iterator() {
    PrimitiveIterator.OfLong iterator = this.parallelStreamSupportMock.iterator();

    verify(this.delegateMock).iterator();
    assertSame(this.iteratorMock, iterator);
  }

  @Override
  @Test
  public void spliterator() {
    Spliterator.OfLong spliterator = this.parallelStreamSupportMock.spliterator();

    verify(this.delegateMock).spliterator();
    assertSame(this.spliteratorMock, spliterator);
  }
}
