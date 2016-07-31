package com.github.ferstl.streams;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
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
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static java.lang.Thread.currentThread;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class ParallelStreamSupportTest {

  private ForkJoinPool workerPool;

  private Stream<String> delegateMock;
  private Stream<?> mappedDelegateMock;
  private IntStream mappedIntDelegateMock;
  private LongStream mappedLongDelegateMock;
  private DoubleStream mappedDoubleDelegateMock;
  private Iterator<?> iteratorMock;
  private Spliterator<?> spliteratorMock;
  private ParallelStreamSupport<String> parallelStreamSupportMock;
  private String[] toArrayResult;

  private Stream<String> delegate;
  private ParallelStreamSupport<String> parallelStreamSupport;


  @Before
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void before() {
    // Precondition for all tests
    assertFalse("This test must not run in a ForkJoinPool", currentThread() instanceof ForkJoinWorkerThread);

    this.workerPool = new ForkJoinPool(1);
    this.delegateMock = mock(Stream.class);
    this.mappedDelegateMock = mock(Stream.class);
    this.mappedIntDelegateMock = mock(IntStream.class);
    this.mappedLongDelegateMock = mock(LongStream.class);
    this.mappedDoubleDelegateMock = mock(DoubleStream.class);
    this.iteratorMock = mock(Iterator.class);
    this.spliteratorMock = mock(Spliterator.class);
    this.toArrayResult = new String[0];

    when(this.delegateMock.map(anyObject())).thenReturn((Stream) this.mappedDelegateMock);
    when(this.delegateMock.mapToInt(anyObject())).thenReturn(this.mappedIntDelegateMock);
    when(this.delegateMock.mapToLong(anyObject())).thenReturn(this.mappedLongDelegateMock);
    when(this.delegateMock.mapToDouble(anyObject())).thenReturn(this.mappedDoubleDelegateMock);
    when(this.delegateMock.flatMap(anyObject())).thenReturn((Stream) this.mappedDelegateMock);
    when(this.delegateMock.flatMapToInt(anyObject())).thenReturn(this.mappedIntDelegateMock);
    when(this.delegateMock.flatMapToLong(anyObject())).thenReturn(this.mappedLongDelegateMock);
    when(this.delegateMock.flatMapToDouble(anyObject())).thenReturn(this.mappedDoubleDelegateMock);
    when(this.delegateMock.iterator()).thenReturn((Iterator) this.iteratorMock);
    when(this.delegateMock.spliterator()).thenReturn((Spliterator) this.spliteratorMock);
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

    this.parallelStreamSupportMock = new ParallelStreamSupport<>(this.delegateMock, this.workerPool);
    this.delegate = singletonList("x").parallelStream();
    this.parallelStreamSupport = new ParallelStreamSupport<>(this.delegate, this.workerPool);
  }

  @After
  public void after() throws InterruptedException {
    this.workerPool.shutdown();
    this.workerPool.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void parallelStream() {
    Collection<?> collection = mock(Collection.class);
    when(collection.parallelStream()).thenReturn(mock(Stream.class));
    ParallelStreamSupport.parallelStream(collection, this.workerPool);

    verify(collection).parallelStream();
  }

  @Test(expected = NullPointerException.class)
  public void parallelStreamNullCollection() {
    ParallelStreamSupport.parallelStream((Collection<?>) null, this.workerPool);
  }

  @Test(expected = NullPointerException.class)
  public void parallelStreamNullWorkerPool() {
    ParallelStreamSupport.parallelStream(new ArrayList<>(), null);
  }

  @Test
  public void parallelStreamWithArray() {
    Stream<String> stream = ParallelStreamSupport.parallelStream(new String[0], this.workerPool);

    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertTrue(stream.isParallel());
  }

  @Test(expected = NullPointerException.class)
  public void parallelStreamWithNullArray() {
    ParallelStreamSupport.parallelStream((String[]) null, this.workerPool);
  }

  @Test
  public void parallelStreamSupportWithSpliterator() {
    List<String> list = new ArrayList<String>();
    Stream<String> stream = ParallelStreamSupport.parallelStream(list.spliterator(), this.workerPool);

    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertTrue(stream.isParallel());
  }

  @Test(expected = NullPointerException.class)
  public void parallelStreamSupportWithNullSpliterator() {
    ParallelStreamSupport.parallelStream((Spliterator<?>) null, this.workerPool);
  }

  @Test
  public void iterator() {
    Iterator<String> iterator = this.parallelStreamSupportMock.iterator();

    verify(this.delegateMock).iterator();
    assertSame(this.iteratorMock, iterator);
  }

  @Test
  public void spliterator() {
    Spliterator<String> spliterator = this.parallelStreamSupportMock.spliterator();

    verify(this.delegateMock).spliterator();
    assertSame(this.spliteratorMock, spliterator);
  }

  @Test
  public void isParallel() {
    when(this.delegateMock.isParallel()).thenReturn(true);
    boolean parallel = this.parallelStreamSupportMock.isParallel();

    verify(this.delegateMock).isParallel();
    assertTrue(parallel);
  }

  @Test
  public void sequential() {
    Stream<String> stream = this.parallelStreamSupportMock.sequential();

    verify(this.delegateMock).sequential();
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void parallel() {
    Stream<String> stream = this.parallelStreamSupportMock.parallel();

    verify(this.delegateMock).parallel();
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void unordered() {
    Stream<String> stream = this.parallelStreamSupportMock.unordered();

    verify(this.delegateMock).unordered();
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void onClose() {
    Runnable r = () -> {};
    Stream<String> stream = this.parallelStreamSupportMock.onClose(r);

    verify(this.delegateMock).onClose(r);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void close() {
    this.parallelStreamSupportMock.close();

    verify(this.delegateMock).close();
  }

  @Test
  public void filter() {
    Predicate<String> p = s -> true;
    Stream<String> stream = this.parallelStreamSupportMock.filter(p);

    verify(this.delegateMock).filter(p);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void map() {
    Function<String, BigDecimal> f = s -> BigDecimal.ONE;
    Stream<BigDecimal> stream = this.parallelStreamSupportMock.map(f);

    verify(this.delegateMock).map(f);
    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertSame(ParallelStreamSupport.class.cast(stream).delegate, this.mappedDelegateMock);
    assertSame(ParallelStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  public void mapToInt() {
    ToIntFunction<String> f = s -> 1;
    IntStream stream = this.parallelStreamSupportMock.mapToInt(f);

    verify(this.delegateMock).mapToInt(f);
    assertThat(stream, instanceOf(ParallelIntStreamSupport.class));
    assertSame(ParallelIntStreamSupport.class.cast(stream).delegate, this.mappedIntDelegateMock);
    assertSame(ParallelIntStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  public void mapToLong() {
    ToLongFunction<String> f = s -> 1L;
    LongStream stream = this.parallelStreamSupportMock.mapToLong(f);

    verify(this.delegateMock).mapToLong(f);
    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertSame(ParallelLongStreamSupport.class.cast(stream).delegate, this.mappedLongDelegateMock);
    assertSame(ParallelLongStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  public void mapToDouble() {
    ToDoubleFunction<String> f = s -> 1.0;
    DoubleStream stream = this.parallelStreamSupportMock.mapToDouble(f);

    verify(this.delegateMock).mapToDouble(f);
    assertThat(stream, instanceOf(ParallelDoubleStreamSupport.class));
    assertSame(ParallelDoubleStreamSupport.class.cast(stream).delegate, this.mappedDoubleDelegateMock);
    assertSame(ParallelDoubleStreamSupport.class.cast(stream).workerPool, this.workerPool);
  }

  @Test
  public void flatMap() {
    Function<String, Stream<BigDecimal>> f = s -> Stream.of(BigDecimal.ONE);
    Stream<BigDecimal> stream = this.parallelStreamSupportMock.flatMap(f);

    verify(this.delegateMock).flatMap(f);
    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertSame(ParallelStreamSupport.class.cast(stream).delegate, this.mappedDelegateMock);
  }


  @Test
  public void flatMapToInt() {
    Function<String, IntStream> f = s -> IntStream.of(1);
    IntStream stream = this.parallelStreamSupportMock.flatMapToInt(f);

    verify(this.delegateMock).flatMapToInt(f);
    assertThat(stream, instanceOf(ParallelIntStreamSupport.class));
    assertSame(ParallelIntStreamSupport.class.cast(stream).delegate, this.mappedIntDelegateMock);
  }

  @Test
  public void flatMapToLong() {
    Function<String, LongStream> f = s -> LongStream.of(1L);
    LongStream stream = this.parallelStreamSupportMock.flatMapToLong(f);

    verify(this.delegateMock).flatMapToLong(f);
    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertSame(ParallelLongStreamSupport.class.cast(stream).delegate, this.mappedLongDelegateMock);
  }

  @Test
  public void flatMapToDouble() {
    Function<String, DoubleStream> f = s -> DoubleStream.of(1L);
    DoubleStream stream = this.parallelStreamSupportMock.flatMapToDouble(f);

    verify(this.delegateMock).flatMapToDouble(f);
    assertThat(stream, instanceOf(ParallelDoubleStreamSupport.class));
    assertSame(ParallelDoubleStreamSupport.class.cast(stream).delegate, this.mappedDoubleDelegateMock);
  }

  @Test
  public void distinct() {
    Stream<String> stream = this.parallelStreamSupportMock.distinct();

    verify(this.delegateMock).distinct();
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void sorted() {
    Stream<String> stream = this.parallelStreamSupportMock.sorted();

    verify(this.delegateMock).sorted();
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void sortedWithComparator() {
    Comparator<String> c = (s1, s2) -> 0;
    Stream<String> stream = this.parallelStreamSupportMock.sorted(c);

    verify(this.delegateMock).sorted(c);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void peek() {
    Consumer<String> c = s -> {};
    Stream<String> stream = this.parallelStreamSupportMock.peek(c);

    verify(this.delegateMock).peek(c);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void limit() {
    Stream<String> stream = this.parallelStreamSupportMock.limit(5);

    verify(this.delegateMock).limit(5);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void skip() {
    Stream<String> stream = this.parallelStreamSupportMock.skip(5);

    verify(this.delegateMock).skip(5);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void forEach() {
    Consumer<String> c = s -> {};
    this.parallelStreamSupportMock.forEach(c);

    verify(this.delegateMock).forEach(c);
  }

  @Test
  public void forEachSequential() {
    this.parallelStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport.forEach(s -> threadRef.set(currentThread()));

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void forEachParallel() {
    this.parallelStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport.forEach(s -> threadRef.set(currentThread()));

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void forEachOrdered() {
    Consumer<String> c = s -> {};
    this.parallelStreamSupportMock.forEachOrdered(c);

    verify(this.delegateMock).forEachOrdered(c);
  }

  @Test
  public void forEachOrderedSequential() {
    this.parallelStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport.forEachOrdered(s -> threadRef.set(currentThread()));

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void forEachOrderedParallel() {
    this.parallelStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport.forEachOrdered(s -> threadRef.set(currentThread()));

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void toArray() {
    Object[] array = this.parallelStreamSupportMock.toArray();

    verify(this.delegateMock).toArray();
    assertSame(this.toArrayResult, array);
  }

  @Test
  public void toArraySequential() {
    this.parallelStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .toArray();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void toArrayParallel() {
    this.parallelStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .toArray();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void toArrayWithGenerator() {
    IntFunction<String[]> generator = i -> new String[i];
    Object[] array = this.parallelStreamSupportMock.toArray(generator);

    verify(this.delegateMock).toArray(generator);
    assertSame(this.toArrayResult, array);
  }


  @Test
  public void toArrayWithGeneratorSequential() {
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
  public void toArrayWithGeneratorParallel() {
    this.parallelStreamSupport.parallel();
    IntFunction<String[]> generator = i -> new String[i];
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .toArray(generator);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void reduceWithIdentityAndAccumulator() {
    BinaryOperator<String> accumulator = (a, b) -> b;
    String result = this.parallelStreamSupportMock.reduce("x", accumulator);

    verify(this.delegateMock).reduce("x", accumulator);
    assertEquals("reduce", result);
  }

  @Test
  public void reduceWithIdentityAndAccumulatorSequential() {
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
  public void reduceWithIdentityAndAccumulatorParallel() {
    this.parallelStreamSupport.parallel();
    BinaryOperator<String> accumulator = (a, b) -> b;
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .reduce("a", accumulator);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void reduceWithAccumulator() {
    BinaryOperator<String> accumulator = (a, b) -> b;
    Optional<String> result = this.parallelStreamSupportMock.reduce(accumulator);

    verify(this.delegateMock).reduce(accumulator);
    assertEquals(Optional.of("reduce"), result);
  }

  @Test
  public void reduceWithAccumulatorSequential() {
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
  public void reduceWithAccumulatorParallel() {
    this.parallelStreamSupport.parallel();
    BinaryOperator<String> accumulator = (a, b) -> b;
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .reduce(accumulator);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }


  @Test
  public void reduceWithIdentityAndAccumulatorAndCombiner() {
    BiFunction<Integer, String, Integer> accumulator = (a, b) -> a;
    BinaryOperator<Integer> combiner = (a, b) -> b;

    Integer result = this.parallelStreamSupportMock.reduce(0, accumulator, combiner);

    verify(this.delegateMock).reduce(0, accumulator, combiner);
    assertEquals((Integer) 42, result);
  }

  @Test
  public void reduceWithIdentityAndAccumulatorAndCombinerSequential() {
    this.parallelStreamSupport.sequential();
    BiFunction<Thread, String, Thread> accumulator = (a, b) -> a;
    BinaryOperator<Thread> combiner = (a, b) -> currentThread();
    Thread thisThread = currentThread();

    Thread result = this.parallelStreamSupport.reduce(currentThread(), accumulator, combiner);

    assertEquals(thisThread, result);
  }

  @Test
  public void reduceWithIdentityAndAccumulatorAndCombinerParallel() {
    this.parallelStreamSupport.parallel();
    BiFunction<Thread, String, Thread> accumulator = (a, b) -> currentThread();
    BinaryOperator<Thread> combiner = (a, b) -> b;

    Thread result = this.parallelStreamSupport.reduce(currentThread(), accumulator, combiner);

    assertThat(result, instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void collectWithSupplierAndAccumulatorAndCombiner() {
    Supplier<Integer> supplier = () -> 1;
    BiConsumer<Integer, String> accumulator = (a, b) -> {};
    BiConsumer<Integer, Integer> combiner = (a, b) -> {};

    Integer result = this.parallelStreamSupportMock.collect(supplier, accumulator, combiner);

    verify(this.delegateMock).collect(supplier, accumulator, combiner);
    assertEquals((Integer) 42, result);
  }

  @Test
  public void collectWithSupplierAndAccumulatorAndCombinerSequential() {
    this.parallelStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void collectWithSupplierAndAccumulatorAndCombinerParallel() {
    this.parallelStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void collectWithCollector() {
    List<String> result = this.parallelStreamSupportMock.collect(toList());

    assertThat(result, contains("collect"));
  }

  @Test
  public void collectWithCollectorSequential() {
    this.parallelStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .collect(toList());

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void collectWithCollectorParallel() {
    this.parallelStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .collect(toList());

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void min() {
    Comparator<String> comparator = (a, b) -> 0;

    Optional<String> result = this.parallelStreamSupportMock.min(comparator);

    verify(this.delegateMock).min(comparator);
    assertEquals(Optional.of("min"), result);
  }

  @Test
  public void minSequential() {
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
  public void minParallel() {
    this.parallelStreamSupport.parallel();
    Comparator<String> comparator = (a, b) -> 0;
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .min(comparator);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void max() {
    Comparator<String> comparator = (a, b) -> 0;

    Optional<String> result = this.parallelStreamSupportMock.max(comparator);

    verify(this.delegateMock).max(comparator);
    assertEquals(Optional.of("max"), result);
  }

  @Test
  public void maxSequential() {
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
  public void maxParallel() {
    this.parallelStreamSupport.parallel();
    Comparator<String> comparator = (a, b) -> 0;
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .max(comparator);

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
    this.parallelStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .count();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void countParallel() {
    this.parallelStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .count();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void anyMatch() {
    Predicate<String> p = s -> true;

    boolean result = this.parallelStreamSupportMock.anyMatch(p);

    verify(this.delegateMock).anyMatch(p);
    assertTrue(result);
  }

  @Test
  public void anyMatchSequential() {
    this.parallelStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .anyMatch(s -> true);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void anyMatchParallel() {
    this.parallelStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .anyMatch(s -> true);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void allMatch() {
    Predicate<String> p = s -> true;

    boolean result = this.parallelStreamSupportMock.allMatch(p);

    verify(this.delegateMock).allMatch(p);
    assertTrue(result);
  }

  @Test
  public void allMatchSequential() {
    this.parallelStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .allMatch(s -> true);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void allMatchParallel() {
    this.parallelStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .allMatch(s -> true);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void noneMatch() {
    Predicate<String> p = s -> true;

    boolean result = this.parallelStreamSupportMock.noneMatch(p);

    verify(this.delegateMock).noneMatch(p);
    assertTrue(result);
  }

  @Test
  public void noneMatchSequential() {
    this.parallelStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .noneMatch(s -> true);

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void noneMatchParallel() {
    this.parallelStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .noneMatch(s -> true);

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void findFirst() {
    Optional<String> result = this.parallelStreamSupportMock.findFirst();

    verify(this.delegateMock).findFirst();
    assertEquals(Optional.of("findFirst"), result);
  }

  @Test
  public void findFirstSequential() {
    this.parallelStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .findFirst();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void findFirstParallel() {
    this.parallelStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .findFirst();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }

  @Test
  public void findAny() {
    Optional<String> result = this.parallelStreamSupportMock.findAny();

    verify(this.delegateMock).findAny();
    assertEquals(Optional.of("findAny"), result);
  }

  @Test
  public void findAnytSequential() {
    this.parallelStreamSupport.sequential();
    Thread thisThread = currentThread();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .findAny();

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void findAnyParallel() {
    this.parallelStreamSupport.parallel();
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport
        .peek(s -> threadRef.set(currentThread()))
        .findAny();

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }
}
