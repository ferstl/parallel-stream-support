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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static java.lang.Thread.currentThread;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Matchers.anyLong;
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

  @BeforeEach
  @SuppressWarnings({"unchecked", "rawtypes"})
  void init() {
    // Precondition for all tests
    assertFalse(currentThread() instanceof ForkJoinWorkerThread, "This test must not run in a ForkJoinPool");

    this.mappedDelegateMock = mock(Stream.class);
    this.mappedIntDelegateMock = mock(IntStream.class);
    this.mappedLongDelegateMock = mock(LongStream.class);
    this.mappedDoubleDelegateMock = mock(DoubleStream.class);
    this.iteratorMock = mock(PrimitiveIterator.OfLong.class);
    this.spliteratorMock = mock(Spliterator.OfLong.class);
    this.toArrayResult = new long[0];
    this.summaryStatistics = new LongSummaryStatistics();

    when(this.delegateMock.map(any())).thenReturn(this.mappedLongDelegateMock);
    when(this.delegateMock.mapToObj(any())).thenReturn((Stream) this.mappedDelegateMock);
    when(this.delegateMock.mapToInt(any())).thenReturn(this.mappedIntDelegateMock);
    when(this.delegateMock.mapToDouble(any())).thenReturn(this.mappedDoubleDelegateMock);
    when(this.delegateMock.flatMap(any())).thenReturn(this.mappedLongDelegateMock);
    when(this.delegateMock.iterator()).thenReturn(this.iteratorMock);
    when(this.delegateMock.spliterator()).thenReturn(this.spliteratorMock);
    when(this.delegateMock.isParallel()).thenReturn(false);
    when(this.delegateMock.toArray()).thenReturn(this.toArrayResult);
    when(this.delegateMock.reduce(anyLong(), any())).thenReturn(42L);
    when(this.delegateMock.reduce(any())).thenReturn(OptionalLong.of(42));
    when(this.delegateMock.collect(any(), any(), any())).thenReturn("collect");
    when(this.delegateMock.sum()).thenReturn(42L);
    when(this.delegateMock.min()).thenReturn(OptionalLong.of(42));
    when(this.delegateMock.max()).thenReturn(OptionalLong.of(42));
    when(this.delegateMock.count()).thenReturn(42L);
    when(this.delegateMock.average()).thenReturn(OptionalDouble.of(42.0));
    when(this.delegateMock.summaryStatistics()).thenReturn(this.summaryStatistics);
    when(this.delegateMock.anyMatch(any())).thenReturn(true);
    when(this.delegateMock.allMatch(any())).thenReturn(true);
    when(this.delegateMock.noneMatch(any())).thenReturn(true);
    when(this.delegateMock.findFirst()).thenReturn(OptionalLong.of(42));
    when(this.delegateMock.findAny()).thenReturn(OptionalLong.of(42));
    when(this.delegateMock.asDoubleStream()).thenReturn(this.mappedDoubleDelegateMock);
    when(this.delegateMock.boxed()).thenReturn((Stream) this.mappedDelegateMock);

    this.delegate = LongStream.of(1L).parallel();
    this.parallelLongStreamSupport = new ParallelLongStreamSupport(this.delegate, this.workerPool);
  }

  @Test
  void parallelStreamWithArray() {
    LongStream stream = ParallelLongStreamSupport.parallelStream(new long[]{42}, this.workerPool);

    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(OptionalLong.of(42), stream.findAny());
  }

  @Test
  void parallelStreamWithNullArray() {
    assertThrows(NullPointerException.class, () -> ParallelLongStreamSupport.parallelStream((long[]) null, this.workerPool));
  }

  @Test
  void parallelStreamSupportWithSpliterator() {
    Spliterator.OfLong spliterator = LongStream.of(42).spliterator();
    LongStream stream = ParallelLongStreamSupport.parallelStream(spliterator, this.workerPool);

    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(OptionalLong.of(42), stream.findAny());
  }

  @Test
  void parallelStreamSupportWithNullSpliterator() {
    assertThrows(NullPointerException.class, () -> ParallelLongStreamSupport.parallelStream((Spliterator.OfLong) null, this.workerPool));
  }

  @Test
  void parallelStreamSupportWithSpliteratorSupplier() {
    Supplier<Spliterator.OfLong> supplier = () -> LongStream.of(42).spliterator();
    LongStream stream = ParallelLongStreamSupport.parallelStream(supplier, 0, this.workerPool);

    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(OptionalLong.of(42), stream.findAny());
  }

  @Test
  void parallelStreamSupportWithNullSpliteratorSupplier() {
    assertThrows(NullPointerException.class, () -> ParallelLongStreamSupport.parallelStream(null, 0, this.workerPool));
  }

  @Test
  void parallelStreamWithBuilder() {
    Builder builder = LongStream.builder();
    builder.accept(42);
    LongStream stream = ParallelLongStreamSupport.parallelStream(builder, this.workerPool);

    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(OptionalLong.of(42), stream.findAny());
  }

  @Test
  void parallelStreamWithNullBuilder() {
    assertThrows(NullPointerException.class, () -> ParallelLongStreamSupport.parallelStream((Builder) null, this.workerPool));
  }

  @Test
  void iterate() {
    LongUnaryOperator operator = a -> a;
    LongStream stream = ParallelLongStreamSupport.iterate(42, operator, this.workerPool);

    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(OptionalLong.of(42), stream.findAny());
  }

  @Test
  void iterateWithNullOperator() {
    assertThrows(NullPointerException.class, () -> ParallelLongStreamSupport.iterate(42, null, this.workerPool));
  }

  @Test
  void generate() {
    LongSupplier supplier = () -> 42;
    LongStream stream = ParallelLongStreamSupport.generate(supplier, this.workerPool);

    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(OptionalLong.of(42), stream.findAny());
  }

  @Test
  void generateWithNullSupplier() {
    assertThrows(NullPointerException.class, () -> ParallelLongStreamSupport.generate(null, this.workerPool));
  }

  @Test
  void range() {
    LongStream stream = ParallelLongStreamSupport.range(0, 5, this.workerPool);

    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertTrue(stream.isParallel());
    assertArrayEquals(stream.toArray(), new long[]{0, 1, 2, 3, 4});
  }

  @Test
  void rangeClosed() {
    LongStream stream = ParallelLongStreamSupport.rangeClosed(0, 5, this.workerPool);

    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertTrue(stream.isParallel());
    assertArrayEquals(stream.toArray(), new long[]{0, 1, 2, 3, 4, 5});
  }

  @Test
  void concat() {
    LongStream a = LongStream.of(42);
    LongStream b = LongStream.of(43);
    LongStream stream = ParallelLongStreamSupport.concat(a, b, this.workerPool);

    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertTrue(stream.isParallel());
    assertArrayEquals(stream.toArray(), new long[]{42, 43});
  }

  @Test
  void concatWithNullStreamA() {
    assertThrows(NullPointerException.class, () -> ParallelLongStreamSupport.concat(null, LongStream.of(42), this.workerPool));
  }

  @Test
  void concatWithNullStreamB() {
    assertThrows(NullPointerException.class, () -> ParallelLongStreamSupport.concat(LongStream.of(42), null, this.workerPool));
  }

  @Test
  void filter() {
    LongPredicate p = i -> true;
    LongStream stream = this.parallelStreamSupportMock.filter(p);

    verify(this.delegateMock).filter(p);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  void map() {
    LongUnaryOperator f = i -> 42;
    LongStream stream = this.parallelStreamSupportMock.map(f);

    verify(this.delegateMock).map(f);
    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertSame(ParallelLongStreamSupport.class.cast(stream).delegate, this.mappedLongDelegateMock);
    assertSame(ParallelLongStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  void mapToObj() {
    LongFunction<String> f = i -> "x";
    Stream<String> stream = this.parallelStreamSupportMock.mapToObj(f);

    verify(this.delegateMock).mapToObj(f);
    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertSame(ParallelStreamSupport.class.cast(stream).delegate, this.mappedDelegateMock);
    assertSame(ParallelStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  void mapToInt() {
    LongToIntFunction f = i -> 1;
    IntStream stream = this.parallelStreamSupportMock.mapToInt(f);

    verify(this.delegateMock).mapToInt(f);
    assertThat(stream, instanceOf(ParallelIntStreamSupport.class));
    assertSame(ParallelIntStreamSupport.class.cast(stream).delegate, this.mappedIntDelegateMock);
    assertSame(ParallelIntStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  void mapToDouble() {
    LongToDoubleFunction f = i -> 1.0;
    DoubleStream stream = this.parallelStreamSupportMock.mapToDouble(f);

    verify(this.delegateMock).mapToDouble(f);
    assertThat(stream, instanceOf(ParallelDoubleStreamSupport.class));
    assertSame(ParallelDoubleStreamSupport.class.cast(stream).delegate, this.mappedDoubleDelegateMock);
    assertSame(ParallelDoubleStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  void flatMap() {
    LongFunction<LongStream> f = i -> LongStream.of(1);
    LongStream stream = this.parallelStreamSupportMock.flatMap(f);

    verify(this.delegateMock).flatMap(f);
    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertSame(ParallelLongStreamSupport.class.cast(stream).delegate, this.mappedLongDelegateMock);
  }

  @Test
  void distinct() {
    LongStream stream = this.parallelStreamSupportMock.distinct();

    verify(this.delegateMock).distinct();
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  void sorted() {
    LongStream stream = this.parallelStreamSupportMock.sorted();

    verify(this.delegateMock).sorted();
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  void peek() {
    LongConsumer c = i -> {
    };
    LongStream stream = this.parallelStreamSupportMock.peek(c);

    verify(this.delegateMock).peek(c);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  void limit() {
    LongStream stream = this.parallelStreamSupportMock.limit(5);

    verify(this.delegateMock).limit(5);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  void skip() {
    LongStream stream = this.parallelStreamSupportMock.skip(5);

    verify(this.delegateMock).skip(5);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  void forEach() {
    LongConsumer c = i -> {
    };
    this.parallelStreamSupportMock.forEach(c);

    verify(this.delegateMock).forEach(c);
  }

  @Test
  void forEachSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport.forEach(i -> threadRef.set(currentThread()));

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void forEachParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport.forEach(i -> threadRef.set(currentThread()));

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void forEachOrdered() {
    LongConsumer c = i -> {
    };
    this.parallelStreamSupportMock.forEachOrdered(c);

    verify(this.delegateMock).forEachOrdered(c);
  }

  @Test
  void forEachOrderedSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport.forEachOrdered(i -> threadRef.set(currentThread()));

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void forEachOrderedParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport.forEachOrdered(i -> threadRef.set(currentThread()));

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void toArray() {
    long[] array = this.parallelStreamSupportMock.toArray();

    verify(this.delegateMock).toArray();
    assertSame(this.toArrayResult, array);
  }

  @Test
  void toArraySequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .toArray();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void toArrayParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .toArray();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void reduceWithIdentityAndAccumulator() {
    LongBinaryOperator accumulator = (a, b) -> b;
    long result = this.parallelStreamSupportMock.reduce(0, accumulator);

    verify(this.delegateMock).reduce(0, accumulator);
    assertEquals(42, result);
  }

  @Test
  void reduceWithIdentityAndAccumulatorSequential() {
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
  void reduceWithIdentityAndAccumulatorParallel() {
    this.parallelLongStreamSupport.parallel();
    LongBinaryOperator accumulator = (a, b) -> b;
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .reduce(0, accumulator);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void reduceWithAccumulator() {
    LongBinaryOperator accumulator = (a, b) -> b;
    OptionalLong result = this.parallelStreamSupportMock.reduce(accumulator);

    verify(this.delegateMock).reduce(accumulator);
    assertEquals(OptionalLong.of(42), result);
  }

  @Test
  void reduceWithAccumulatorSequential() {
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
  void reduceWithAccumulatorParallel() {
    this.parallelLongStreamSupport.parallel();
    LongBinaryOperator accumulator = (a, b) -> b;
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .reduce(accumulator);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void collectWithSupplierAndAccumulatorAndCombiner() {
    Supplier<String> supplier = () -> "x";
    ObjLongConsumer<String> accumulator = (a, b) -> {
    };
    BiConsumer<String, String> combiner = (a, b) -> {
    };

    String result = this.parallelStreamSupportMock.collect(supplier, accumulator, combiner);

    verify(this.delegateMock).collect(supplier, accumulator, combiner);
    assertEquals("collect", result);
  }

  @Test
  void collectWithSupplierAndAccumulatorAndCombinerSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void collectWithSupplierAndAccumulatorAndCombinerParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void sum() {
    long result = this.parallelStreamSupportMock.sum();

    verify(this.delegateMock).sum();
    assertEquals(42, result);
  }

  @Test
  void sumSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .sum();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void sumParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .sum();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void min() {
    OptionalLong result = this.parallelStreamSupportMock.min();

    verify(this.delegateMock).min();
    assertEquals(OptionalLong.of(42), result);
  }

  @Test
  void minSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .min();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void minParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .min();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void max() {
    OptionalLong result = this.parallelStreamSupportMock.max();

    verify(this.delegateMock).max();
    assertEquals(OptionalLong.of(42), result);
  }

  @Test
  void maxSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .max();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void maxParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .max();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void count() {
    long count = this.parallelStreamSupportMock.count();

    verify(this.delegateMock).count();
    assertEquals(42, count);
  }

  @Test
  void countSequential() {
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
  void countParallel() {
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
  void average() {
    OptionalDouble result = this.parallelStreamSupportMock.average();

    verify(this.delegateMock).average();
    assertEquals(OptionalDouble.of(42.0), result);
  }

  @Test
  void averageSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .average();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void averageParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .average();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void summaryStatistics() {
    LongSummaryStatistics result = this.parallelStreamSupportMock.summaryStatistics();

    verify(this.delegateMock).summaryStatistics();
    assertEquals(this.summaryStatistics, result);
  }

  @Test
  void summaryStatisticsSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .summaryStatistics();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void summaryStatisticsParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .summaryStatistics();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void anyMatch() {
    LongPredicate p = i -> true;

    boolean result = this.parallelStreamSupportMock.anyMatch(p);

    verify(this.delegateMock).anyMatch(p);
    assertTrue(result);
  }

  @Test
  void anyMatchSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .anyMatch(i -> true);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void anyMatchParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .anyMatch(i -> true);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void allMatch() {
    LongPredicate p = i -> true;

    boolean result = this.parallelStreamSupportMock.allMatch(p);

    verify(this.delegateMock).allMatch(p);
    assertTrue(result);
  }

  @Test
  void allMatchSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .allMatch(i -> true);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void allMatchParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .allMatch(i -> true);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void noneMatch() {
    LongPredicate p = i -> true;

    boolean result = this.parallelStreamSupportMock.noneMatch(p);

    verify(this.delegateMock).noneMatch(p);
    assertTrue(result);
  }

  @Test
  void noneMatchSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .noneMatch(i -> true);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void noneMatchParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .noneMatch(i -> true);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void findFirst() {
    OptionalLong result = this.parallelStreamSupportMock.findFirst();

    verify(this.delegateMock).findFirst();
    assertEquals(OptionalLong.of(42), result);
  }

  @Test
  void findFirstSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .findFirst();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void findFirstParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .findFirst();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void findAny() {
    OptionalLong result = this.parallelStreamSupportMock.findAny();

    verify(this.delegateMock).findAny();
    assertEquals(OptionalLong.of(42), result);
  }

  @Test
  void findAnytSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .findAny();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void findAnyParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .findAny();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void asDoubleStream() {
    DoubleStream stream = this.parallelStreamSupportMock.asDoubleStream();

    verify(this.delegateMock).asDoubleStream();
    assertThat(stream, instanceOf(ParallelDoubleStreamSupport.class));
    assertSame(this.mappedDoubleDelegateMock, ParallelDoubleStreamSupport.class.cast(stream).delegate);
    assertSame(this.workerPool, ParallelDoubleStreamSupport.class.cast(stream).workerPool);
  }

  @Test
  void boxed() {
    Stream<Long> stream = this.parallelStreamSupportMock.boxed();

    verify(this.delegateMock).boxed();
    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertSame(this.mappedDelegateMock, ParallelStreamSupport.class.cast(stream).delegate);
    assertSame(this.workerPool, ParallelStreamSupport.class.cast(stream).workerPool);
  }

  @Override
  @Test
  void iterator() {
    PrimitiveIterator.OfLong iterator = this.parallelStreamSupportMock.iterator();

    verify(this.delegateMock).iterator();
    assertSame(this.iteratorMock, iterator);
  }

  @Override
  @Test
  void spliterator() {
    Spliterator.OfLong spliterator = this.parallelStreamSupportMock.spliterator();

    verify(this.delegateMock).spliterator();
    assertSame(this.spliteratorMock, spliterator);
  }
}
