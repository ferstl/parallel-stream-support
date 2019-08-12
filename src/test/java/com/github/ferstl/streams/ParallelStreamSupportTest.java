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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicReference;
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
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.Stream.Builder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static java.lang.Thread.currentThread;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class ParallelStreamSupportTest extends AbstractParallelStreamSupportTest<String, Stream<String>, ParallelStreamSupport<String>> {

  private Stream<?> mappedDelegateMock;
  private IntStream mappedIntDelegateMock;
  private LongStream mappedLongDelegateMock;
  private DoubleStream mappedDoubleDelegateMock;
  private String[] toArrayResult;

  private Stream<String> delegate;
  private ParallelStreamSupport<String> parallelStreamSupport;

  @Override
  @SuppressWarnings("unchecked")
  protected ParallelStreamSupport<String> createParallelStreamSupportMock(ForkJoinPool workerPool) {
    return new ParallelStreamSupport<>(mock(Stream.class), workerPool);
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
    this.toArrayResult = new String[0];

    when(this.delegateMock.map(anyObject())).thenReturn((Stream) this.mappedDelegateMock);
    when(this.delegateMock.mapToInt(anyObject())).thenReturn(this.mappedIntDelegateMock);
    when(this.delegateMock.mapToLong(anyObject())).thenReturn(this.mappedLongDelegateMock);
    when(this.delegateMock.mapToDouble(anyObject())).thenReturn(this.mappedDoubleDelegateMock);
    when(this.delegateMock.flatMap(anyObject())).thenReturn((Stream) this.mappedDelegateMock);
    when(this.delegateMock.flatMapToInt(anyObject())).thenReturn(this.mappedIntDelegateMock);
    when(this.delegateMock.flatMapToLong(anyObject())).thenReturn(this.mappedLongDelegateMock);
    when(this.delegateMock.flatMapToDouble(anyObject())).thenReturn(this.mappedDoubleDelegateMock);
    when(this.delegateMock.isParallel()).thenReturn(false);
    when(this.delegateMock.toArray()).thenReturn(this.toArrayResult);
    when(this.delegateMock.toArray(anyObject())).thenReturn(this.toArrayResult);
    when(this.delegateMock.reduce(anyString(), anyObject())).thenReturn("reduce");
    when(this.delegateMock.reduce(anyObject())).thenReturn(Optional.of("reduce"));
    when(this.delegateMock.reduce(anyObject(), anyObject(), anyObject())).thenReturn(42);
    when(this.delegateMock.collect(anyObject(), anyObject(), anyObject())).thenReturn(42);
    when(this.delegateMock.collect(anyObject())).thenReturn(singletonList("collect"));
    when(this.delegateMock.min(anyObject())).thenReturn(Optional.of("min"));
    when(this.delegateMock.max(anyObject())).thenReturn(Optional.of("max"));
    when(this.delegateMock.count()).thenReturn(42L);
    when(this.delegateMock.anyMatch(anyObject())).thenReturn(true);
    when(this.delegateMock.allMatch(anyObject())).thenReturn(true);
    when(this.delegateMock.noneMatch(anyObject())).thenReturn(true);
    when(this.delegateMock.findFirst()).thenReturn(Optional.of("findFirst"));
    when(this.delegateMock.findAny()).thenReturn(Optional.of("findAny"));

    this.delegate = singletonList("x").parallelStream();
    this.parallelStreamSupport = new ParallelStreamSupport<>(this.delegate, this.workerPool);
  }

  @Test
  void parallelStream() {
    Stream<String> stream = ParallelStreamSupport.parallelStream(Collections.singletonList("a"), this.workerPool);

    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(Optional.of("a"), stream.findAny());
  }

  @Test
  void parallelStreamNullCollection() {
    assertThrows(NullPointerException.class, () -> ParallelStreamSupport.parallelStream((Collection<?>) null, this.workerPool));
  }

  @Test
  void parallelStreamNullWorkerPool() {
    assertThrows(NullPointerException.class, () -> ParallelStreamSupport.parallelStream(new ArrayList<>(), null));
  }

  @Test
  void parallelStreamWithArray() {
    Stream<String> stream = ParallelStreamSupport.parallelStream(new String[]{"a"}, this.workerPool);

    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(Optional.of("a"), stream.findAny());
  }

  @Test
  void parallelStreamWithNullArray() {
    assertThrows(NullPointerException.class, () -> ParallelStreamSupport.parallelStream((String[]) null, this.workerPool));
  }

  @Test
  void parallelStreamSupportWithSpliterator() {
    List<String> list = singletonList("a");
    Stream<String> stream = ParallelStreamSupport.parallelStream(list.spliterator(), this.workerPool);

    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(Optional.of("a"), stream.findAny());
  }

  @Test
  void parallelStreamSupportWithNullSpliterator() {
    assertThrows(NullPointerException.class, () -> ParallelStreamSupport.parallelStream((Spliterator<?>) null, this.workerPool));
  }

  @Test
  void parallelStreamSupportWithSpliteratorSupplier() {
    Supplier<Spliterator<String>> supplier = () -> singletonList("a").spliterator();
    Stream<String> stream = ParallelStreamSupport.parallelStream(supplier, 0, this.workerPool);

    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(Optional.of("a"), stream.findAny());
  }

  @Test
  void parallelStreamSupportWithNullSpliteratorSupplier() {
    assertThrows(NullPointerException.class, () -> ParallelStreamSupport.parallelStream(null, 0, this.workerPool));
  }

  @Test
  void parallelStreamWithBuilder() {
    Builder<String> builder = Stream.builder();
    builder.accept("a");
    Stream<String> stream = ParallelStreamSupport.parallelStream(builder, this.workerPool);

    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(Optional.of("a"), stream.findAny());
  }

  @Test
  void parallelStreamWithNullBuilder() {
    Builder<String> builder = null;
    assertThrows(NullPointerException.class, () -> ParallelStreamSupport.parallelStream(builder, this.workerPool));
  }

  @Test
  void iterate() {
    UnaryOperator<String> operator = a -> a;
    Stream<String> stream = ParallelStreamSupport.iterate("a", operator, this.workerPool);

    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(Optional.of("a"), stream.findAny());
  }

  @Test
  void iterateWithNullOperator() {
    assertThrows(NullPointerException.class, () -> ParallelStreamSupport.iterate("a", null, this.workerPool));
  }

  @Test
  void generate() {
    Supplier<String> supplier = () -> "a";
    Stream<String> stream = ParallelStreamSupport.generate(supplier, this.workerPool);

    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertTrue(stream.isParallel());
    assertEquals(Optional.of("a"), stream.findAny());
  }

  @Test
  void generateWithNullSupplier() {
    assertThrows(NullPointerException.class, () -> ParallelStreamSupport.generate(null, this.workerPool));
  }

  @Test
  void concat() {
    Stream<String> a = Stream.of("a");
    Stream<String> b = Stream.of("b");
    Stream<String> stream = ParallelStreamSupport.concat(a, b, this.workerPool);

    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertTrue(stream.isParallel());
    assertThat(stream.collect(toList()), contains("a", "b"));
  }

  @Test
  void concatWithNullStreamA() {
    assertThrows(NullPointerException.class, () -> ParallelStreamSupport.concat(null, Stream.of("b"), this.workerPool));
  }

  @Test
  void concatWithNullStreamB() {
    assertThrows(NullPointerException.class, () -> ParallelStreamSupport.concat(Stream.of("a"), null, this.workerPool));
  }

  @Test
  void filter() {
    Predicate<String> p = s -> true;
    Stream<String> stream = this.parallelStreamSupportMock.filter(p);

    verify(this.delegateMock).filter(p);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  void map() {
    Function<String, BigDecimal> f = s -> BigDecimal.ONE;
    Stream<BigDecimal> stream = this.parallelStreamSupportMock.map(f);

    verify(this.delegateMock).map(f);
    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertSame(ParallelStreamSupport.class.cast(stream).delegate, this.mappedDelegateMock);
    assertSame(ParallelStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  void mapToInt() {
    ToIntFunction<String> f = s -> 1;
    IntStream stream = this.parallelStreamSupportMock.mapToInt(f);

    verify(this.delegateMock).mapToInt(f);
    assertThat(stream, instanceOf(ParallelIntStreamSupport.class));
    assertSame(ParallelIntStreamSupport.class.cast(stream).delegate, this.mappedIntDelegateMock);
    assertSame(ParallelIntStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  void mapToLong() {
    ToLongFunction<String> f = s -> 1L;
    LongStream stream = this.parallelStreamSupportMock.mapToLong(f);

    verify(this.delegateMock).mapToLong(f);
    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertSame(ParallelLongStreamSupport.class.cast(stream).delegate, this.mappedLongDelegateMock);
    assertSame(ParallelLongStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  void mapToDouble() {
    ToDoubleFunction<String> f = s -> 1.0;
    DoubleStream stream = this.parallelStreamSupportMock.mapToDouble(f);

    verify(this.delegateMock).mapToDouble(f);
    assertThat(stream, instanceOf(ParallelDoubleStreamSupport.class));
    assertSame(ParallelDoubleStreamSupport.class.cast(stream).delegate, this.mappedDoubleDelegateMock);
    assertSame(ParallelDoubleStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  void flatMap() {
    Function<String, Stream<BigDecimal>> f = s -> Stream.of(BigDecimal.ONE);
    Stream<BigDecimal> stream = this.parallelStreamSupportMock.flatMap(f);

    verify(this.delegateMock).flatMap(f);
    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertSame(ParallelStreamSupport.class.cast(stream).delegate, this.mappedDelegateMock);
  }


  @Test
  void flatMapToInt() {
    Function<String, IntStream> f = s -> IntStream.of(1);
    IntStream stream = this.parallelStreamSupportMock.flatMapToInt(f);

    verify(this.delegateMock).flatMapToInt(f);
    assertThat(stream, instanceOf(ParallelIntStreamSupport.class));
    assertSame(ParallelIntStreamSupport.class.cast(stream).delegate, this.mappedIntDelegateMock);
  }

  @Test
  void flatMapToLong() {
    Function<String, LongStream> f = s -> LongStream.of(1L);
    LongStream stream = this.parallelStreamSupportMock.flatMapToLong(f);

    verify(this.delegateMock).flatMapToLong(f);
    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertSame(ParallelLongStreamSupport.class.cast(stream).delegate, this.mappedLongDelegateMock);
  }

  @Test
  void flatMapToDouble() {
    Function<String, DoubleStream> f = s -> DoubleStream.of(1L);
    DoubleStream stream = this.parallelStreamSupportMock.flatMapToDouble(f);

    verify(this.delegateMock).flatMapToDouble(f);
    assertThat(stream, instanceOf(ParallelDoubleStreamSupport.class));
    assertSame(ParallelDoubleStreamSupport.class.cast(stream).delegate, this.mappedDoubleDelegateMock);
  }

  @Test
  void distinct() {
    Stream<String> stream = this.parallelStreamSupportMock.distinct();

    verify(this.delegateMock).distinct();
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  void sorted() {
    Stream<String> stream = this.parallelStreamSupportMock.sorted();

    verify(this.delegateMock).sorted();
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  void sortedWithComparator() {
    Comparator<String> c = (s1, s2) -> 0;
    Stream<String> stream = this.parallelStreamSupportMock.sorted(c);

    verify(this.delegateMock).sorted(c);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  void peek() {
    Consumer<String> c = s -> {
    };
    Stream<String> stream = this.parallelStreamSupportMock.peek(c);

    verify(this.delegateMock).peek(c);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  void limit() {
    Stream<String> stream = this.parallelStreamSupportMock.limit(5);

    verify(this.delegateMock).limit(5);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  void skip() {
    Stream<String> stream = this.parallelStreamSupportMock.skip(5);

    verify(this.delegateMock).skip(5);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  void forEach() {
    Consumer<String> c = s -> {
    };
    this.parallelStreamSupportMock.forEach(c);

    verify(this.delegateMock).forEach(c);
  }

  @Test
  void forEachSequential() {
    this.parallelStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport.forEach(s -> threadRef.set(currentThread()));

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void forEachParallel() {
    this.parallelStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport.forEach(s -> threadRef.set(currentThread()));

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void forEachOrdered() {
    Consumer<String> c = s -> {
    };
    this.parallelStreamSupportMock.forEachOrdered(c);

    verify(this.delegateMock).forEachOrdered(c);
  }

  @Test
  void forEachOrderedSequential() {
    this.parallelStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport.forEachOrdered(s -> threadRef.set(currentThread()));

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void forEachOrderedParallel() {
    this.parallelStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport.forEachOrdered(s -> threadRef.set(currentThread()));

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void toArray() {
    Object[] array = this.parallelStreamSupportMock.toArray();

    verify(this.delegateMock).toArray();
    assertSame(this.toArrayResult, array);
  }

  @Test
  void toArraySequential() {
    this.parallelStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .toArray();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void toArrayParallel() {
    this.parallelStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .toArray();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void toArrayWithGenerator() {
    IntFunction<String[]> generator = i -> new String[i];
    Object[] array = this.parallelStreamSupportMock.toArray(generator);

    verify(this.delegateMock).toArray(generator);
    assertSame(this.toArrayResult, array);
  }


  @Test
  void toArrayWithGeneratorSequential() {
    this.parallelStreamSupport.sequential();
    IntFunction<String[]> generator = i -> new String[i];
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .toArray(generator);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void toArrayWithGeneratorParallel() {
    this.parallelStreamSupport.parallel();
    IntFunction<String[]> generator = i -> new String[i];
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .toArray(generator);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void reduceWithIdentityAndAccumulator() {
    BinaryOperator<String> accumulator = (a, b) -> b;
    String result = this.parallelStreamSupportMock.reduce("x", accumulator);

    verify(this.delegateMock).reduce("x", accumulator);
    assertEquals("reduce", result);
  }

  @Test
  void reduceWithIdentityAndAccumulatorSequential() {
    this.parallelStreamSupport.sequential();
    BinaryOperator<String> accumulator = (a, b) -> b;
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .reduce("a", accumulator);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void reduceWithIdentityAndAccumulatorParallel() {
    this.parallelStreamSupport.parallel();
    BinaryOperator<String> accumulator = (a, b) -> b;
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .reduce("a", accumulator);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void reduceWithAccumulator() {
    BinaryOperator<String> accumulator = (a, b) -> b;
    Optional<String> result = this.parallelStreamSupportMock.reduce(accumulator);

    verify(this.delegateMock).reduce(accumulator);
    assertEquals(Optional.of("reduce"), result);
  }

  @Test
  void reduceWithAccumulatorSequential() {
    this.parallelStreamSupport.sequential();
    BinaryOperator<String> accumulator = (a, b) -> b;
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .reduce(accumulator);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void reduceWithAccumulatorParallel() {
    this.parallelStreamSupport.parallel();
    BinaryOperator<String> accumulator = (a, b) -> b;
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .reduce(accumulator);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }


  @Test
  void reduceWithIdentityAndAccumulatorAndCombiner() {
    BiFunction<Integer, String, Integer> accumulator = (a, b) -> a;
    BinaryOperator<Integer> combiner = (a, b) -> b;

    Integer result = this.parallelStreamSupportMock.reduce(0, accumulator, combiner);

    verify(this.delegateMock).reduce(0, accumulator, combiner);
    assertEquals((Integer) 42, result);
  }

  @Test
  void reduceWithIdentityAndAccumulatorAndCombinerSequential() {
    this.parallelStreamSupport.sequential();
    BiFunction<Thread, String, Thread> accumulator = (a, b) -> a;
    BinaryOperator<Thread> combiner = (a, b) -> currentThread();
    Thread thisThread = currentThread();

    Thread result = this.parallelStreamSupport.reduce(currentThread(), accumulator, combiner);

    assertEquals(thisThread, result);
  }

  @Test
  void reduceWithIdentityAndAccumulatorAndCombinerParallel() {
    this.parallelStreamSupport.parallel();
    BiFunction<Thread, String, Thread> accumulator = (a, b) -> currentThread();
    BinaryOperator<Thread> combiner = (a, b) -> b;

    Thread result = this.parallelStreamSupport.reduce(currentThread(), accumulator, combiner);

    assertThat(result, instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void collectWithSupplierAndAccumulatorAndCombiner() {
    Supplier<Integer> supplier = () -> 1;
    BiConsumer<Integer, String> accumulator = (a, b) -> {
    };
    BiConsumer<Integer, Integer> combiner = (a, b) -> {
    };

    Integer result = this.parallelStreamSupportMock.collect(supplier, accumulator, combiner);

    verify(this.delegateMock).collect(supplier, accumulator, combiner);
    assertEquals((Integer) 42, result);
  }

  @Test
  void collectWithSupplierAndAccumulatorAndCombinerSequential() {
    this.parallelStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void collectWithSupplierAndAccumulatorAndCombinerParallel() {
    this.parallelStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void collectWithCollector() {
    List<String> result = this.parallelStreamSupportMock.collect(toList());

    assertThat(result, contains("collect"));
  }

  @Test
  void collectWithCollectorSequential() {
    this.parallelStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .collect(toList());

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void collectWithCollectorParallel() {
    this.parallelStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .collect(toList());

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void min() {
    Comparator<String> comparator = (a, b) -> 0;

    Optional<String> result = this.parallelStreamSupportMock.min(comparator);

    verify(this.delegateMock).min(comparator);
    assertEquals(Optional.of("min"), result);
  }

  @Test
  void minSequential() {
    this.parallelStreamSupport.sequential();
    Comparator<String> comparator = (a, b) -> 0;
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .min(comparator);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void minParallel() {
    this.parallelStreamSupport.parallel();
    Comparator<String> comparator = (a, b) -> 0;
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .min(comparator);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void max() {
    Comparator<String> comparator = (a, b) -> 0;

    Optional<String> result = this.parallelStreamSupportMock.max(comparator);

    verify(this.delegateMock).max(comparator);
    assertEquals(Optional.of("max"), result);
  }

  @Test
  void maxSequential() {
    this.parallelStreamSupport.sequential();
    Comparator<String> comparator = (a, b) -> 0;
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .max(comparator);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void maxParallel() {
    this.parallelStreamSupport.parallel();
    Comparator<String> comparator = (a, b) -> 0;
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .max(comparator);

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
    this.parallelStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        // Don't use peek() in combination with count(). See Javadoc.
        .filter(s -> {
          threadRef.set(currentThread());
          return true;
        }).count();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void countParallel() {
    this.parallelStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        // Don't use peek() in combination with count(). See Javadoc.
        .filter(s -> {
          threadRef.set(currentThread());
          return true;
        }).count();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void anyMatch() {
    Predicate<String> p = s -> true;

    boolean result = this.parallelStreamSupportMock.anyMatch(p);

    verify(this.delegateMock).anyMatch(p);
    assertTrue(result);
  }

  @Test
  void anyMatchSequential() {
    this.parallelStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .anyMatch(s -> true);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void anyMatchParallel() {
    this.parallelStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .anyMatch(s -> true);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void allMatch() {
    Predicate<String> p = s -> true;

    boolean result = this.parallelStreamSupportMock.allMatch(p);

    verify(this.delegateMock).allMatch(p);
    assertTrue(result);
  }

  @Test
  void allMatchSequential() {
    this.parallelStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .allMatch(s -> true);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void allMatchParallel() {
    this.parallelStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .allMatch(s -> true);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void noneMatch() {
    Predicate<String> p = s -> true;

    boolean result = this.parallelStreamSupportMock.noneMatch(p);

    verify(this.delegateMock).noneMatch(p);
    assertTrue(result);
  }

  @Test
  void noneMatchSequential() {
    this.parallelStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .noneMatch(s -> true);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void noneMatchParallel() {
    this.parallelStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .noneMatch(s -> true);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void findFirst() {
    Optional<String> result = this.parallelStreamSupportMock.findFirst();

    verify(this.delegateMock).findFirst();
    assertEquals(Optional.of("findFirst"), result);
  }

  @Test
  void findFirstSequential() {
    this.parallelStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .findFirst();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void findFirstParallel() {
    this.parallelStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .findFirst();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  void findAny() {
    Optional<String> result = this.parallelStreamSupportMock.findAny();

    verify(this.delegateMock).findAny();
    assertEquals(Optional.of("findAny"), result);
  }

  @Test
  void findAnytSequential() {
    this.parallelStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .findAny();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  void findAnyParallel() {
    this.parallelStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .findAny();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }
}
