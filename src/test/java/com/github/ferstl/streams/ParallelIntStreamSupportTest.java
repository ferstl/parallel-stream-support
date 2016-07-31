package com.github.ferstl.streams;

import java.util.ArrayList;
import java.util.OptionalInt;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class ParallelIntStreamSupportTest {

  private ForkJoinPool workerPool;

  private IntStream delegateMock;
  private Stream<?> mappedDelegateMock;
  private IntStream mappedIntDelegateMock;
  private LongStream mappedLongDelegateMock;
  private DoubleStream mappedDoubleDelegateMock;
  private PrimitiveIterator.OfInt iteratorMock;
  private Spliterator.OfInt spliteratorMock;
  private ParallelIntStreamSupport parallelIntStreamSupportMock;
  private int[] toArrayResult;

  private IntStream delegate;
  private ParallelIntStreamSupport parallelIntStreamSupport;


  @Before
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void before() {
    // Precondition for all tests
    assertFalse("This test must not run in a ForkJoinPool", currentThread() instanceof ForkJoinWorkerThread);

    this.workerPool = new ForkJoinPool(1);
    this.delegateMock = mock(IntStream.class);
    this.mappedDelegateMock = mock(Stream.class);
    this.mappedIntDelegateMock = mock(IntStream.class);
    this.mappedLongDelegateMock = mock(LongStream.class);
    this.mappedDoubleDelegateMock = mock(DoubleStream.class);
    this.iteratorMock = mock(PrimitiveIterator.OfInt.class);
    this.spliteratorMock = mock(Spliterator.OfInt.class);
    this.toArrayResult = new int[0];

    when(this.delegateMock.map(anyObject())).thenReturn(this.mappedIntDelegateMock);
    when(this.delegateMock.mapToObj(anyObject())).thenReturn((Stream) this.mappedDelegateMock);
    when(this.delegateMock.mapToLong(anyObject())).thenReturn(this.mappedLongDelegateMock);
    when(this.delegateMock.mapToDouble(anyObject())).thenReturn(this.mappedDoubleDelegateMock);
    when(this.delegateMock.flatMap(anyObject())).thenReturn(this.mappedIntDelegateMock);
    when(this.delegateMock.iterator()).thenReturn(this.iteratorMock);
    when(this.delegateMock.spliterator()).thenReturn(this.spliteratorMock);
    when(this.delegateMock.isParallel()).thenReturn(false);
    when(this.delegateMock.toArray()).thenReturn(this.toArrayResult);
    when(this.delegateMock.reduce(anyInt(), anyObject())).thenReturn(42);
    when(this.delegateMock.reduce(anyObject())).thenReturn(OptionalInt.of(42));
    when(this.delegateMock.collect(anyObject(), anyObject(), anyObject())).thenReturn("collect");
    when(this.delegateMock.min()).thenReturn(OptionalInt.of(42));
    when(this.delegateMock.max()).thenReturn(OptionalInt.of(42));
    when(this.delegateMock.count()).thenReturn(42L);
    when(this.delegateMock.anyMatch(anyObject())).thenReturn(true);
    when(this.delegateMock.allMatch(anyObject())).thenReturn(true);
    when(this.delegateMock.noneMatch(anyObject())).thenReturn(true);
    when(this.delegateMock.findFirst()).thenReturn(OptionalInt.of(42));
    when(this.delegateMock.findAny()).thenReturn(OptionalInt.of(42));

    this.parallelIntStreamSupportMock = new ParallelIntStreamSupport(this.delegateMock, this.workerPool);
    this.delegate = IntStream.of(1).parallel();
    this.parallelIntStreamSupport = new ParallelIntStreamSupport(this.delegate, this.workerPool);
  }

  @After
  public void after() throws InterruptedException {
    this.workerPool.shutdown();
    this.workerPool.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  public void isParallel() {
    when(this.delegateMock.isParallel()).thenReturn(true);
    boolean parallel = this.parallelIntStreamSupportMock.isParallel();

    verify(this.delegateMock).isParallel();
    assertTrue(parallel);
  }

  @Test
  public void unordered() {
    IntStream stream = this.parallelIntStreamSupportMock.unordered();

    verify(this.delegateMock).unordered();
    assertSame(this.parallelIntStreamSupportMock, stream);
  }

  @Test
  public void onClose() {
    Runnable r = () -> {};
    IntStream stream = this.parallelIntStreamSupportMock.onClose(r);

    verify(this.delegateMock).onClose(r);
    assertSame(this.parallelIntStreamSupportMock, stream);
  }

  @Test
  public void close() {
    this.parallelIntStreamSupportMock.close();

    verify(this.delegateMock).close();
  }

  @Test
  public void filter() {
    IntPredicate p = i -> true;
    IntStream stream = this.parallelIntStreamSupportMock.filter(p);

    verify(this.delegateMock).filter(p);
    assertSame(this.parallelIntStreamSupportMock, stream);
  }

  @Test
  public void map() {
    IntUnaryOperator f = i -> 42;
    IntStream stream = this.parallelIntStreamSupportMock.map(f);

    verify(this.delegateMock).map(f);
    assertThat(stream, instanceOf(ParallelIntStreamSupport.class));
    assertSame(ParallelIntStreamSupport.class.cast(stream).delegate, this.mappedIntDelegateMock);
    assertSame(ParallelIntStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  public void mapToObj() {
    IntFunction<String> f = i -> "x";
    Stream<String> stream = this.parallelIntStreamSupportMock.mapToObj(f);

    verify(this.delegateMock).mapToObj(f);
    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertSame(ParallelStreamSupport.class.cast(stream).delegate, this.mappedDelegateMock);
    assertSame(ParallelStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  public void mapToLong() {
    IntToLongFunction f = i -> 1L;
    LongStream stream = this.parallelIntStreamSupportMock.mapToLong(f);

    verify(this.delegateMock).mapToLong(f);
    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertSame(ParallelLongStreamSupport.class.cast(stream).delegate, this.mappedLongDelegateMock);
    assertSame(ParallelLongStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  public void mapToDouble() {
    IntToDoubleFunction f = i -> 1.0;
    DoubleStream stream = this.parallelIntStreamSupportMock.mapToDouble(f);

    verify(this.delegateMock).mapToDouble(f);
    assertThat(stream, instanceOf(ParallelDoubleStreamSupport.class));
    assertSame(ParallelDoubleStreamSupport.class.cast(stream).delegate, this.mappedDoubleDelegateMock);
    assertSame(ParallelDoubleStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  public void flatMap() {
    IntFunction<IntStream> f = i -> IntStream.of(1);
    IntStream stream = this.parallelIntStreamSupportMock.flatMap(f);

    verify(this.delegateMock).flatMap(f);
    assertThat(stream, instanceOf(ParallelIntStreamSupport.class));
    assertSame(ParallelIntStreamSupport.class.cast(stream).delegate, this.mappedIntDelegateMock);
  }

  @Test
  public void distinct() {
    IntStream stream = this.parallelIntStreamSupportMock.distinct();

    verify(this.delegateMock).distinct();
    assertSame(this.parallelIntStreamSupportMock, stream);
  }

  @Test
  public void sorted() {
    IntStream stream = this.parallelIntStreamSupportMock.sorted();

    verify(this.delegateMock).sorted();
    assertSame(this.parallelIntStreamSupportMock, stream);
  }

  @Test
  public void peek() {
    IntConsumer c = i -> {};
    IntStream stream = this.parallelIntStreamSupportMock.peek(c);

    verify(this.delegateMock).peek(c);
    assertSame(this.parallelIntStreamSupportMock, stream);
  }

  @Test
  public void limit() {
    IntStream stream = this.parallelIntStreamSupportMock.limit(5);

    verify(this.delegateMock).limit(5);
    assertSame(this.parallelIntStreamSupportMock, stream);
  }

  @Test
  public void skip() {
    IntStream stream = this.parallelIntStreamSupportMock.skip(5);

    verify(this.delegateMock).skip(5);
    assertSame(this.parallelIntStreamSupportMock, stream);
  }

  @Test
  public void forEach() {
    IntConsumer c = i -> {};
    this.parallelIntStreamSupportMock.forEach(c);

    verify(this.delegateMock).forEach(c);
  }

  @Test
  public void forEachSequential() {
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport.forEach(i -> threadRef.set(currentThread()));

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void forEachParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport.forEach(i -> threadRef.set(currentThread()));

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void forEachOrdered() {
    IntConsumer c = i -> {};
    this.parallelIntStreamSupportMock.forEachOrdered(c);

    verify(this.delegateMock).forEachOrdered(c);
  }

  @Test
  public void forEachOrderedSequential() {
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport.forEachOrdered(i -> threadRef.set(currentThread()));

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void forEachOrderedParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport.forEachOrdered(i -> threadRef.set(currentThread()));

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void toArray() {
    int[] array = this.parallelIntStreamSupportMock.toArray();

    verify(this.delegateMock).toArray();
    assertSame(this.toArrayResult, array);
  }

  @Test
  public void toArraySequential() {
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .toArray();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void toArrayParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .toArray();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void reduceWithIdentityAndAccumulator() {
    IntBinaryOperator accumulator = (a, b) -> b;
    int result = this.parallelIntStreamSupportMock.reduce(0, accumulator);

    verify(this.delegateMock).reduce(0, accumulator);
    assertEquals(42, result);
  }

  @Test
  public void reduceWithIdentityAndAccumulatorSequential() {
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
  public void reduceWithIdentityAndAccumulatorParallel() {
    this.parallelIntStreamSupport.parallel();
    IntBinaryOperator accumulator = (a, b) -> b;
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .reduce(0, accumulator);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void reduceWithAccumulator() {
    IntBinaryOperator accumulator = (a, b) -> b;
    OptionalInt result = this.parallelIntStreamSupportMock.reduce(accumulator);

    verify(this.delegateMock).reduce(accumulator);
    assertEquals(OptionalInt.of(42), result);
  }

  @Test
  public void reduceWithAccumulatorSequential() {
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
  public void reduceWithAccumulatorParallel() {
    this.parallelIntStreamSupport.parallel();
    IntBinaryOperator accumulator = (a, b) -> b;
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .reduce(accumulator);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void collectWithSupplierAndAccumulatorAndCombiner() {
    Supplier<String> supplier = () -> "x";
    ObjIntConsumer<String> accumulator = (a, b) -> {};
    BiConsumer<String, String> combiner = (a, b) -> {};

    String result = this.parallelIntStreamSupportMock.collect(supplier, accumulator, combiner);

    verify(this.delegateMock).collect(supplier, accumulator, combiner);
    assertEquals("collect", result);
  }

  @Test
  public void collectWithSupplierAndAccumulatorAndCombinerSequential() {
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void collectWithSupplierAndAccumulatorAndCombinerParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void min() {
    OptionalInt result = this.parallelIntStreamSupportMock.min();

    verify(this.delegateMock).min();
    assertEquals(OptionalInt.of(42), result);
  }

  @Test
  public void minSequential() {
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .min();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void minParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .min();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void max() {
    OptionalInt result = this.parallelIntStreamSupportMock.max();

    verify(this.delegateMock).max();
    assertEquals(OptionalInt.of(42), result);
  }

  @Test
  public void maxSequential() {
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .max();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void maxParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .max();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void count() {
    long count = this.parallelIntStreamSupportMock.count();

    verify(this.delegateMock).count();
    assertEquals(42L, count);
  }

  @Test
  public void countSequential() {
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .count();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void countParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .count();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void anyMatch() {
    IntPredicate p = i -> true;

    boolean result = this.parallelIntStreamSupportMock.anyMatch(p);

    verify(this.delegateMock).anyMatch(p);
    assertTrue(result);
  }

  @Test
  public void anyMatchSequential() {
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .anyMatch(i -> true);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void anyMatchParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .anyMatch(i -> true);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void allMatch() {
    IntPredicate p = i -> true;

    boolean result = this.parallelIntStreamSupportMock.allMatch(p);

    verify(this.delegateMock).allMatch(p);
    assertTrue(result);
  }

  @Test
  public void allMatchSequential() {
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .allMatch(i -> true);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void allMatchParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .allMatch(i -> true);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void noneMatch() {
    IntPredicate p = i -> true;

    boolean result = this.parallelIntStreamSupportMock.noneMatch(p);

    verify(this.delegateMock).noneMatch(p);
    assertTrue(result);
  }

  @Test
  public void noneMatchSequential() {
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .noneMatch(i -> true);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void noneMatchParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .noneMatch(i -> true);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void findFirst() {
    OptionalInt result = this.parallelIntStreamSupportMock.findFirst();

    verify(this.delegateMock).findFirst();
    assertEquals(OptionalInt.of(42), result);
  }

  @Test
  public void findFirstSequential() {
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .findFirst();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void findFirstParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .findFirst();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void findAny() {
    OptionalInt result = this.parallelIntStreamSupportMock.findAny();

    verify(this.delegateMock).findAny();
    assertEquals(OptionalInt.of(42), result);
  }

  @Test
  public void findAnytSequential() {
    this.parallelIntStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .findAny();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void findAnyParallel() {
    this.parallelIntStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelIntStreamSupport
        .peek(i -> threadRef.set(currentThread()))
        .findAny();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void sequential() {
    IntStream stream = this.parallelIntStreamSupportMock.sequential();

    verify(this.delegateMock).sequential();
    assertSame(this.parallelIntStreamSupportMock, stream);
  }

  @Test
  public void parallel() {
    IntStream stream = this.parallelIntStreamSupportMock.parallel();

    verify(this.delegateMock).parallel();
    assertSame(this.parallelIntStreamSupportMock, stream);
  }

  @Test
  public void iterator() {
    PrimitiveIterator.OfInt iterator = this.parallelIntStreamSupportMock.iterator();

    verify(this.delegateMock).iterator();
    assertSame(this.iteratorMock, iterator);
  }

  @Test
  public void spliterator() {
    Spliterator.OfInt spliterator = this.parallelIntStreamSupportMock.spliterator();

    verify(this.delegateMock).spliterator();
    assertSame(this.spliteratorMock, spliterator);
  }
}
