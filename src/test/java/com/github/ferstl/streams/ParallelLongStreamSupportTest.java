package com.github.ferstl.streams;

import java.util.ArrayList;
import java.util.LongSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static java.lang.Thread.currentThread;
import static org.hamcrest.Matchers.instanceOf;
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


public class ParallelLongStreamSupportTest {

  private ForkJoinPool workerPool;

  private LongStream delegateMock;
  private Stream<?> mappedDelegateMock;
  private IntStream mappedIntDelegateMock;
  private LongStream mappedLongDelegateMock;
  private DoubleStream mappedDoubleDelegateMock;
  private PrimitiveIterator.OfLong iteratorMock;
  private Spliterator.OfLong spliteratorMock;
  private ParallelLongStreamSupport parallelLongStreamSupportMock;
  private long[] toArrayResult;
  private LongSummaryStatistics summaryStatistics;

  private LongStream delegate;
  private ParallelLongStreamSupport parallelLongStreamSupport;


  @Before
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void before() {
    // Precondition for all tests
    assertFalse("This test must not run in a ForkJoinPool", currentThread() instanceof ForkJoinWorkerThread);

    this.workerPool = new ForkJoinPool(1);
    this.delegateMock = mock(LongStream.class);
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

    this.parallelLongStreamSupportMock = new ParallelLongStreamSupport(this.delegateMock, this.workerPool);
    this.delegate = LongStream.of(1L).parallel();
    this.parallelLongStreamSupport = new ParallelLongStreamSupport(this.delegate, this.workerPool);
  }

  @After
  public void after() throws InterruptedException {
    this.workerPool.shutdown();
    this.workerPool.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  public void isParallel() {
    when(this.delegateMock.isParallel()).thenReturn(true);
    boolean parallel = this.parallelLongStreamSupportMock.isParallel();

    verify(this.delegateMock).isParallel();
    assertTrue(parallel);
  }

  @Test
  public void unordered() {
    LongStream stream = this.parallelLongStreamSupportMock.unordered();

    verify(this.delegateMock).unordered();
    assertSame(this.parallelLongStreamSupportMock, stream);
  }

  @Test
  public void onClose() {
    Runnable r = () -> {};
    LongStream stream = this.parallelLongStreamSupportMock.onClose(r);

    verify(this.delegateMock).onClose(r);
    assertSame(this.parallelLongStreamSupportMock, stream);
  }

  @Test
  public void close() {
    this.parallelLongStreamSupportMock.close();

    verify(this.delegateMock).close();
  }

  @Test
  public void filter() {
    LongPredicate p = i -> true;
    LongStream stream = this.parallelLongStreamSupportMock.filter(p);

    verify(this.delegateMock).filter(p);
    assertSame(this.parallelLongStreamSupportMock, stream);
  }

  @Test
  public void map() {
    LongUnaryOperator f = i -> 42;
    LongStream stream = this.parallelLongStreamSupportMock.map(f);

    verify(this.delegateMock).map(f);
    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertSame(ParallelLongStreamSupport.class.cast(stream).delegate, this.mappedLongDelegateMock);
    assertSame(ParallelLongStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  public void mapToObj() {
    LongFunction<String> f = i -> "x";
    Stream<String> stream = this.parallelLongStreamSupportMock.mapToObj(f);

    verify(this.delegateMock).mapToObj(f);
    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertSame(ParallelStreamSupport.class.cast(stream).delegate, this.mappedDelegateMock);
    assertSame(ParallelStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  public void mapToInt() {
    LongToIntFunction f = i -> 1;
    IntStream stream = this.parallelLongStreamSupportMock.mapToInt(f);

    verify(this.delegateMock).mapToInt(f);
    assertThat(stream, instanceOf(ParallelIntStreamSupport.class));
    assertSame(ParallelIntStreamSupport.class.cast(stream).delegate, this.mappedIntDelegateMock);
    assertSame(ParallelIntStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  public void mapToDouble() {
    LongToDoubleFunction f = i -> 1.0;
    DoubleStream stream = this.parallelLongStreamSupportMock.mapToDouble(f);

    verify(this.delegateMock).mapToDouble(f);
    assertThat(stream, instanceOf(ParallelDoubleStreamSupport.class));
    assertSame(ParallelDoubleStreamSupport.class.cast(stream).delegate, this.mappedDoubleDelegateMock);
    assertSame(ParallelDoubleStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  public void flatMap() {
    LongFunction<LongStream> f = i -> LongStream.of(1);
    LongStream stream = this.parallelLongStreamSupportMock.flatMap(f);

    verify(this.delegateMock).flatMap(f);
    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertSame(ParallelLongStreamSupport.class.cast(stream).delegate, this.mappedLongDelegateMock);
  }

  @Test
  public void distinct() {
    LongStream stream = this.parallelLongStreamSupportMock.distinct();

    verify(this.delegateMock).distinct();
    assertSame(this.parallelLongStreamSupportMock, stream);
  }

  @Test
  public void sorted() {
    LongStream stream = this.parallelLongStreamSupportMock.sorted();

    verify(this.delegateMock).sorted();
    assertSame(this.parallelLongStreamSupportMock, stream);
  }

  @Test
  public void peek() {
    LongConsumer c = i -> {};
    LongStream stream = this.parallelLongStreamSupportMock.peek(c);

    verify(this.delegateMock).peek(c);
    assertSame(this.parallelLongStreamSupportMock, stream);
  }

  @Test
  public void limit() {
    LongStream stream = this.parallelLongStreamSupportMock.limit(5);

    verify(this.delegateMock).limit(5);
    assertSame(this.parallelLongStreamSupportMock, stream);
  }

  @Test
  public void skip() {
    LongStream stream = this.parallelLongStreamSupportMock.skip(5);

    verify(this.delegateMock).skip(5);
    assertSame(this.parallelLongStreamSupportMock, stream);
  }

  @Test
  public void forEach() {
    LongConsumer c = i -> {};
    this.parallelLongStreamSupportMock.forEach(c);

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
    this.parallelLongStreamSupportMock.forEachOrdered(c);

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
    long[] array = this.parallelLongStreamSupportMock.toArray();

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
    long result = this.parallelLongStreamSupportMock.reduce(0, accumulator);

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
    OptionalLong result = this.parallelLongStreamSupportMock.reduce(accumulator);

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

    String result = this.parallelLongStreamSupportMock.collect(supplier, accumulator, combiner);

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
    long result = this.parallelLongStreamSupportMock.sum();

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
    OptionalLong result = this.parallelLongStreamSupportMock.min();

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
    OptionalLong result = this.parallelLongStreamSupportMock.max();

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
    long count = this.parallelLongStreamSupportMock.count();

    verify(this.delegateMock).count();
    assertEquals(42, count);
  }

  @Test
  public void countSequential() {
    this.parallelLongStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .count();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void countParallel() {
    this.parallelLongStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelLongStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .count();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void average() {
    OptionalDouble result = this.parallelLongStreamSupportMock.average();

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
    LongSummaryStatistics result = this.parallelLongStreamSupportMock.summaryStatistics();

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

    boolean result = this.parallelLongStreamSupportMock.anyMatch(p);

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

    boolean result = this.parallelLongStreamSupportMock.allMatch(p);

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

    boolean result = this.parallelLongStreamSupportMock.noneMatch(p);

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
    OptionalLong result = this.parallelLongStreamSupportMock.findFirst();

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
    OptionalLong result = this.parallelLongStreamSupportMock.findAny();

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
    DoubleStream stream = this.parallelLongStreamSupportMock.asDoubleStream();

    verify(this.delegateMock).asDoubleStream();
    assertThat(stream, instanceOf(ParallelDoubleStreamSupport.class));
    assertSame(this.mappedDoubleDelegateMock, ParallelDoubleStreamSupport.class.cast(stream).delegate);
    assertSame(this.workerPool, ParallelDoubleStreamSupport.class.cast(stream).workerPool);
  }

  @Test
  public void boxed() {
    Stream<Long> stream = this.parallelLongStreamSupportMock.boxed();

    verify(this.delegateMock).boxed();
    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertSame(this.mappedDelegateMock, ParallelStreamSupport.class.cast(stream).delegate);
    assertSame(this.workerPool, ParallelStreamSupport.class.cast(stream).workerPool);
  }

  @Test
  public void sequential() {
    LongStream stream = this.parallelLongStreamSupportMock.sequential();

    verify(this.delegateMock).sequential();
    assertSame(this.parallelLongStreamSupportMock, stream);
  }

  @Test
  public void parallel() {
    LongStream stream = this.parallelLongStreamSupportMock.parallel();

    verify(this.delegateMock).parallel();
    assertSame(this.parallelLongStreamSupportMock, stream);
  }

  @Test
  public void iterator() {
    PrimitiveIterator.OfLong iterator = this.parallelLongStreamSupportMock.iterator();

    verify(this.delegateMock).iterator();
    assertSame(this.iteratorMock, iterator);
  }

  @Test
  public void spliterator() {
    Spliterator.OfLong spliterator = this.parallelLongStreamSupportMock.spliterator();

    verify(this.delegateMock).spliterator();
    assertSame(this.spliteratorMock, spliterator);
  }
}
