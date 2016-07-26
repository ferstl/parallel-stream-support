package com.github.ferstl.streams;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
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
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
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
    ToDoubleFunction<String> f = s -> 1D;
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
  public void forEachSequencial() {
    this.parallelStreamSupport.sequential();
    Thread thisThread = Thread.currentThread();
    // Used to write from the Lambda
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport.forEach(s -> threadRef.set(currentThread()));

    assertEquals(thisThread, threadRef.get());
  }

  @Test
  public void forEachParallel() {
    this.parallelStreamSupport.parallel();
    // Used to write from the Lambda
    AtomicReference<Thread> threadRef = new AtomicReference<>();

    this.parallelStreamSupport.forEach(s -> threadRef.set(currentThread()));

    assertThat(threadRef.get(), instanceOf(ForkJoinWorkerThread.class));
  }
}
