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
import java.util.IntSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicReference;
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
import java.util.stream.IntStream.Builder;
import java.util.stream.LongStream;
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
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class ParallelIntStreamSupportTest extends AbstractParallelStreamSupportTest<Integer, IntStream, ParallelIntStreamSupport> {

  private Stream<?> mappedDelegateMock;
  private IntStream mappedIntDelegateMock;
  private LongStream mappedLongDelegateMock;
  private DoubleStream mappedDoubleDelegateMock;
  private PrimitiveIterator.OfInt iteratorMock;
  private Spliterator.OfInt spliteratorMock;
  private int[] toArrayResult;
  private IntSummaryStatistics summaryStatistics;

  private IntStream delegate;
  private ParallelIntStreamSupport parallelIntStreamSupport;


  @Override
  protected ParallelIntStreamSupport createParallelStreamSupportMock(ForkJoinPool workerPool) {
    return new ParallelIntStreamSupport(mock(IntStream.class), workerPool);
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
    this.iteratorMock = mock(PrimitiveIterator.OfInt.class);
    this.spliteratorMock = mock(Spliterator.OfInt.class);
    this.toArrayResult = new int[0];
    this.summaryStatistics = new IntSummaryStatistics();

    when(this.delegateMock.map(any())).thenReturn(this.mappedIntDelegateMock);
    when(this.delegateMock.mapToObj(any())).thenReturn((Stream) this.mappedDelegateMock);
    when(this.delegateMock.mapToLong(any())).thenReturn(this.mappedLongDelegateMock);
    when(this.delegateMock.mapToDouble(any())).thenReturn(this.mappedDoubleDelegateMock);
    when(this.delegateMock.flatMap(any())).thenReturn(this.mappedIntDelegateMock);
    when(this.delegateMock.iterator()).thenReturn(this.iteratorMock);
    when(this.delegateMock.spliterator()).thenReturn(this.spliteratorMock);
    when(this.delegateMock.isParallel()).thenReturn(false);
    when(this.delegateMock.toArray()).thenReturn(this.toArrayResult);
    when(this.delegateMock.reduce(anyInt(), any())).thenReturn(42);
    when(this.delegateMock.reduce(any())).thenReturn(OptionalInt.of(42));
    when(this.delegateMock.collect(any(), any(), any())).thenReturn("collect");
    when(this.delegateMock.sum()).thenReturn(42);
    when(this.delegateMock.min()).thenReturn(OptionalInt.of(42));
    when(this.delegateMock.max()).thenReturn(OptionalInt.of(42));
    when(this.delegateMock.count()).thenReturn(42L);
    when(this.delegateMock.average()).thenReturn(OptionalDouble.of(42.0));
    when(this.delegateMock.summaryStatistics()).thenReturn(this.summaryStatistics);
    when(this.delegateMock.anyMatch(any())).thenReturn(true);
    when(this.delegateMock.allMatch(any())).thenReturn(true);
    when(this.delegateMock.noneMatch(any())).thenReturn(true);
    when(this.delegateMock.findFirst()).thenReturn(OptionalInt.of(42));
    when(this.delegateMock.findAny()).thenReturn(OptionalInt.of(42));
    when(this.delegateMock.asLongStream()).thenReturn(this.mappedLongDelegateMock);
    when(this.delegateMock.asDoubleStream()).thenReturn(this.mappedDoubleDelegateMock);
    when(this.delegateMock.boxed()).thenReturn((Stream) this.mappedDelegateMock);

    this.delegate = IntStream.of(1).parallel();
    this.parallelIntStreamSupport = new ParallelIntStreamSupport(this.delegate, this.workerPool);
  }

  @Test
  void parallelStreamWithArray() {
    IntStream stream = ParallelIntStreamSupport.parallelStream(new int[]{42}, this.workerPool);

    assertThat(stream, instanceOf(ParallelIntStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(OptionalInt.of(42), stream.findAny());
  }

  @Test
  void parallelStreamWithNullArray() {
    assertThrows(NullPointerException.class, () -> ParallelIntStreamSupport.parallelStream((int[]) null, this.workerPool));
  }

  @Test
  void parallelStreamSupportWithSpliterator() {
    Spliterator.OfInt spliterator = IntStream.of(42).spliterator();
    IntStream stream = ParallelIntStreamSupport.parallelStream(spliterator, this.workerPool);

    assertThat(stream, instanceOf(ParallelIntStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(OptionalInt.of(42), stream.findAny());
  }

  @Test
  void parallelStreamSupportWithNullSpliterator() {
    assertThrows(NullPointerException.class, () -> ParallelIntStreamSupport.parallelStream((Spliterator.OfInt) null, this.workerPool));
  }

  @Test
  void parallelStreamSupportWithSpliteratorSupplier() {
    Supplier<Spliterator.OfInt> supplier = () -> IntStream.of(42).spliterator();
    IntStream stream = ParallelIntStreamSupport.parallelStream(supplier, 0, this.workerPool);

    assertThat(stream, instanceOf(ParallelIntStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(OptionalInt.of(42), stream.findAny());
  }

  @Test
  void parallelStreamSupportWithNullSpliteratorSupplier() {
    assertThrows(NullPointerException.class, () -> ParallelIntStreamSupport.parallelStream(null, 0, this.workerPool));
  }

  @Test
  void parallelStreamWithBuilder() {
    Builder builder = IntStream.builder();
    builder.accept(42);
    IntStream stream = ParallelIntStreamSupport.parallelStream(builder, this.workerPool);

    assertThat(stream, instanceOf(ParallelIntStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(OptionalInt.of(42), stream.findAny());
  }

  @Test
  void parallelStreamWithNullBuilder() {
    assertThrows(NullPointerException.class, () -> ParallelIntStreamSupport.parallelStream((Builder) null, this.workerPool));
  }

  @Test
  void iterate() {
    IntUnaryOperator operator = a -> a;
    IntStream stream = ParallelIntStreamSupport.iterate(42, operator, this.workerPool);

    assertThat(stream, instanceOf(ParallelIntStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(OptionalInt.of(42), stream.findAny());
  }

  @Test
  void iterateWithNullOperator() {
    assertThrows(NullPointerException.class, () -> ParallelIntStreamSupport.iterate(42, null, this.workerPool));
  }

  @Test
  void generate() {
    IntSupplier supplier = () -> 42;
    IntStream stream = ParallelIntStreamSupport.generate(supplier, this.workerPool);

    assertThat(stream, instanceOf(ParallelIntStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(OptionalInt.of(42), stream.findAny());
  }

  @Test
  void generateWithNullSupplier() {
    assertThrows(NullPointerException.class, () -> ParallelIntStreamSupport.generate(null, this.workerPool));
  }

  @Test
  void range() {
    IntStream stream = ParallelIntStreamSupport.range(0, 5, this.workerPool);

    assertThat(stream, instanceOf(ParallelIntStreamSupport.class));
    assertTrue(stream.isParallel());
    assertArrayEquals(stream.toArray(), new int[]{0, 1, 2, 3, 4});
  }

  @Test
  void rangeClosed() {
    IntStream stream = ParallelIntStreamSupport.rangeClosed(0, 5, this.workerPool);

    assertThat(stream, instanceOf(ParallelIntStreamSupport.class));
    assertTrue(stream.isParallel());
    assertArrayEquals(stream.toArray(), new int[]{0, 1, 2, 3, 4, 5});
  }

  @Test
  void concat() {
    IntStream a = IntStream.of(42);
    IntStream b = IntStream.of(43);
    IntStream stream = ParallelIntStreamSupport.concat(a, b, this.workerPool);

    assertThat(stream, instanceOf(ParallelIntStreamSupport.class));
    assertTrue(stream.isParallel());
    assertArrayEquals(stream.toArray(), new int[]{42, 43});
  }

  @Test
  void concatWithNullStreamA() {
    assertThrows(NullPointerException.class, () -> ParallelIntStreamSupport.concat(null, IntStream.of(42), this.workerPool));
  }

  @Test
  void concatWithNullStreamB() {
    assertThrows(NullPointerException.class, () -> ParallelIntStreamSupport.concat(IntStream.of(42), null, this.workerPool));
  }

  @Test
  void filter() {
    IntPredicate p = i -> true;
    IntStream stream = this.parallelStreamSupportMock.filter(p);

    verify(this.delegateMock).filter(p);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  void map() {
    IntUnaryOperator f = i -> 42;
    IntStream stream = this.parallelStreamSupportMock.map(f);

    verify(this.delegateMock).map(f);
    assertThat(stream, instanceOf(ParallelIntStreamSupport.class));
    assertSame(ParallelIntStreamSupport.class.cast(stream).delegate, this.mappedIntDelegateMock);
    assertSame(ParallelIntStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  void mapToObj() {
    IntFunction<String> f = i -> "x";
    Stream<String> stream = this.parallelStreamSupportMock.mapToObj(f);

    verify(this.delegateMock).mapToObj(f);
    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertSame(ParallelStreamSupport.class.cast(stream).delegate, this.mappedDelegateMock);
    assertSame(ParallelStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  void mapToLong() {
    IntToLongFunction f = i -> 1L;
    LongStream stream = this.parallelStreamSupportMock.mapToLong(f);

    verify(this.delegateMock).mapToLong(f);
    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertSame(ParallelLongStreamSupport.class.cast(stream).delegate, this.mappedLongDelegateMock);
    assertSame(ParallelLongStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  void mapToDouble() {
    IntToDoubleFunction f = i -> 1.0;
    DoubleStream stream = this.parallelStreamSupportMock.mapToDouble(f);

    verify(this.delegateMock).mapToDouble(f);
    assertThat(stream, instanceOf(ParallelDoubleStreamSupport.class));
    assertSame(ParallelDoubleStreamSupport.class.cast(stream).delegate, this.mappedDoubleDelegateMock);
    assertSame(ParallelDoubleStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  void flatMap() {
    IntFunction<IntStream> f = i -> IntStream.of(1);
    IntStream stream = this.parallelStreamSupportMock.flatMap(f);

    verify(this.delegateMock).flatMap(f);
    assertThat(stream, instanceOf(ParallelIntStreamSupport.class));
    assertSame(ParallelIntStreamSupport.class.cast(stream).delegate, this.mappedIntDelegateMock);
  }

  @Test
  void distinct() {
    IntStream stream = this.parallelStreamSupportMock.distinct();

    verify(this.delegateMock).distinct();
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  void sorted() {
    IntStream stream = this.parallelStreamSupportMock.sorted();

    verify(this.delegateMock).sorted();
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  void peek() {
    IntConsumer c = i -> {
    };
    IntStream stream = this.parallelStreamSupportMock.peek(c);

    verify(this.delegateMock).peek(c);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  void limit() {
    IntStream stream = this.parallelStreamSupportMock.limit(5);

    verify(this.delegateMock).limit(5);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  void skip() {
    IntStream stream = this.parallelStreamSupportMock.skip(5);

    verify(this.delegateMock).skip(5);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  void forEach() {
    IntConsumer c = i -> {
    };
    this.parallelStreamSupportMock.forEach(c);

    verify(this.delegateMock).forEach(c);
  }

  @Test
  void forEachSequential() {
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport.forEach(i -> threadRef.set(currentThread()));

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void forEachParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport.forEach(i -> threadRef.set(currentThread()));

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void forEachOrdered() {
    IntConsumer c = i -> {
    };
    this.parallelStreamSupportMock.forEachOrdered(c);

    verify(this.delegateMock).forEachOrdered(c);
  }

  @Test
  void forEachOrderedSequential() {
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport.forEachOrdered(i -> threadRef.set(currentThread()));

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void forEachOrderedParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport.forEachOrdered(i -> threadRef.set(currentThread()));

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void toArray() {
    int[] array = this.parallelStreamSupportMock.toArray();

    verify(this.delegateMock).toArray();
    assertSame(this.toArrayResult, array);
  }

  @Test
  void toArraySequential() {
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .toArray();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void toArrayParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .toArray();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void reduceWithIdentityAndAccumulator() {
    IntBinaryOperator accumulator = (a, b) -> b;
    int result = this.parallelStreamSupportMock.reduce(0, accumulator);

    verify(this.delegateMock).reduce(0, accumulator);
    assertEquals(42, result);
  }

  @Test
  void reduceWithIdentityAndAccumulatorSequential() {
    this.parallelIntStreamSupport.sequential();
    IntBinaryOperator accumulator = (a, b) -> b;
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .reduce(0, accumulator);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void reduceWithIdentityAndAccumulatorParallel() {
    this.parallelIntStreamSupport.parallel();
    IntBinaryOperator accumulator = (a, b) -> b;
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .reduce(0, accumulator);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void reduceWithAccumulator() {
    IntBinaryOperator accumulator = (a, b) -> b;
    OptionalInt result = this.parallelStreamSupportMock.reduce(accumulator);

    verify(this.delegateMock).reduce(accumulator);
    assertEquals(OptionalInt.of(42), result);
  }

  @Test
  void reduceWithAccumulatorSequential() {
    this.parallelIntStreamSupport.sequential();
    IntBinaryOperator accumulator = (a, b) -> b;
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .reduce(accumulator);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void reduceWithAccumulatorParallel() {
    this.parallelIntStreamSupport.parallel();
    IntBinaryOperator accumulator = (a, b) -> b;
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .reduce(accumulator);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void collectWithSupplierAndAccumulatorAndCombiner() {
    Supplier<String> supplier = () -> "x";
    ObjIntConsumer<String> accumulator = (a, b) -> {
    };
    BiConsumer<String, String> combiner = (a, b) -> {
    };

    String result = this.parallelStreamSupportMock.collect(supplier, accumulator, combiner);

    verify(this.delegateMock).collect(supplier, accumulator, combiner);
    assertEquals("collect", result);
  }

  @Test
  void collectWithSupplierAndAccumulatorAndCombinerSequential() {
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void collectWithSupplierAndAccumulatorAndCombinerParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void sum() {
    int result = this.parallelStreamSupportMock.sum();

    verify(this.delegateMock).sum();
    assertEquals(42, result);
  }

  @Test
  void sumSequential() {
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .sum();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void sumParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .sum();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void min() {
    OptionalInt result = this.parallelStreamSupportMock.min();

    verify(this.delegateMock).min();
    assertEquals(OptionalInt.of(42), result);
  }

  @Test
  void minSequential() {
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .min();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void minParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .min();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void max() {
    OptionalInt result = this.parallelStreamSupportMock.max();

    verify(this.delegateMock).max();
    assertEquals(OptionalInt.of(42), result);
  }

  @Test
  void maxSequential() {
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .max();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void maxParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .max();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void count() {
    long count = this.parallelStreamSupportMock.count();

    verify(this.delegateMock).count();
    assertEquals(42L, count);
  }

  @Test
  void countSequential() {
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .filter(i -> {
          // Don't use peek() in combination with count(). See Javadoc.
          threadRef.set(currentThread());
          return true;
        }).count();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void countParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
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
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .average();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void averageParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .average();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void summaryStatistics() {
    IntSummaryStatistics result = this.parallelStreamSupportMock.summaryStatistics();

    verify(this.delegateMock).summaryStatistics();
    assertEquals(this.summaryStatistics, result);
  }

  @Test
  void summaryStatisticsSequential() {
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .summaryStatistics();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void summaryStatisticsParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .summaryStatistics();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void anyMatch() {
    IntPredicate p = i -> true;

    boolean result = this.parallelStreamSupportMock.anyMatch(p);

    verify(this.delegateMock).anyMatch(p);
    assertTrue(result);
  }

  @Test
  void anyMatchSequential() {
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .anyMatch(i -> true);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void anyMatchParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .anyMatch(i -> true);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void allMatch() {
    IntPredicate p = i -> true;

    boolean result = this.parallelStreamSupportMock.allMatch(p);

    verify(this.delegateMock).allMatch(p);
    assertTrue(result);
  }

  @Test
  void allMatchSequential() {
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .allMatch(i -> true);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void allMatchParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .allMatch(i -> true);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void noneMatch() {
    IntPredicate p = i -> true;

    boolean result = this.parallelStreamSupportMock.noneMatch(p);

    verify(this.delegateMock).noneMatch(p);
    assertTrue(result);
  }

  @Test
  void noneMatchSequential() {
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .noneMatch(i -> true);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void noneMatchParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .noneMatch(i -> true);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void findFirst() {
    OptionalInt result = this.parallelStreamSupportMock.findFirst();

    verify(this.delegateMock).findFirst();
    assertEquals(OptionalInt.of(42), result);
  }

  @Test
  void findFirstSequential() {
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .findFirst();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void findFirstParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .findFirst();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void findAny() {
    OptionalInt result = this.parallelStreamSupportMock.findAny();

    verify(this.delegateMock).findAny();
    assertEquals(OptionalInt.of(42), result);
  }

  @Test
  void findAnytSequential() {
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .findAny();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void findAnyParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .findAny();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void asLongStream() {
    LongStream stream = this.parallelStreamSupportMock.asLongStream();

    verify(this.delegateMock).asLongStream();
    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertSame(this.mappedLongDelegateMock, ParallelLongStreamSupport.class.cast(stream).delegate);
    assertSame(this.workerPool, ParallelLongStreamSupport.class.cast(stream).workerPool);
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
    Stream<Integer> stream = this.parallelStreamSupportMock.boxed();

    verify(this.delegateMock).boxed();
    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertSame(this.mappedDelegateMock, ParallelStreamSupport.class.cast(stream).delegate);
    assertSame(this.workerPool, ParallelStreamSupport.class.cast(stream).workerPool);
  }

  @Override
  @Test
  void iterator() {
    PrimitiveIterator.OfInt iterator = this.parallelStreamSupportMock.iterator();

    verify(this.delegateMock).iterator();
    assertSame(this.iteratorMock, iterator);
  }

  @Override
  @Test
  void spliterator() {
    Spliterator.OfInt spliterator = this.parallelStreamSupportMock.spliterator();

    verify(this.delegateMock).spliterator();
    assertSame(this.spliteratorMock, spliterator);
  }
}
