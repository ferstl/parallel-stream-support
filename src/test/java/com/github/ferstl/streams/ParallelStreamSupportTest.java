package com.github.ferstl.streams;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
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
import static org.hamcrest.Matchers.instanceOf;
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

  private Stream<String> delegate;
  private Stream<?> mappedDelegate;
  private IntStream mappedIntDelegate;
  private LongStream mappedLongDelegate;
  private DoubleStream mappedDoubleDelegate;
  private Iterator<?> iterator;
  private Spliterator<?> spliterator;
  private ParallelStreamSupport<String> parallelStreamSupport;


  @Before
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void before() {
    // Precondition for all tests
    assertFalse("This test must not run in a ForkJoinPool", currentThread() instanceof ForkJoinWorkerThread);

    this.workerPool = new ForkJoinPool(1);
    this.delegate = mock(Stream.class);
    this.mappedDelegate = mock(Stream.class);
    this.mappedIntDelegate = mock(IntStream.class);
    this.mappedLongDelegate = mock(LongStream.class);
    this.mappedDoubleDelegate = mock(DoubleStream.class);
    this.iterator = mock(Iterator.class);
    this.spliterator = mock(Spliterator.class);

    when(this.delegate.map(anyObject())).thenReturn((Stream) this.mappedDelegate);
    when(this.delegate.mapToInt(anyObject())).thenReturn(this.mappedIntDelegate);
    when(this.delegate.mapToLong(anyObject())).thenReturn(this.mappedLongDelegate);
    when(this.delegate.mapToDouble(anyObject())).thenReturn(this.mappedDoubleDelegate);
    when(this.delegate.flatMap(anyObject())).thenReturn((Stream) this.mappedDelegate);
    when(this.delegate.flatMapToInt(anyObject())).thenReturn(this.mappedIntDelegate);
    when(this.delegate.flatMapToLong(anyObject())).thenReturn(this.mappedLongDelegate);
    when(this.delegate.flatMapToDouble(anyObject())).thenReturn(this.mappedDoubleDelegate);
    when(this.delegate.iterator()).thenReturn((Iterator) this.iterator);
    when(this.delegate.spliterator()).thenReturn((Spliterator) this.spliterator);
    when(this.delegate.isParallel()).thenReturn(true);

    this.parallelStreamSupport = new ParallelStreamSupport<>(this.delegate, this.workerPool);
  }

  @After
  public void after() throws InterruptedException {
    this.workerPool.shutdown();
    this.workerPool.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  public void iterator() {
    Iterator<String> iterator = this.parallelStreamSupport.iterator();

    verify(this.delegate).iterator();
    assertSame(this.iterator, iterator);
  }

  @Test
  public void spliterator() {
    Spliterator<String> spliterator = this.parallelStreamSupport.spliterator();

    verify(this.delegate).spliterator();
    assertSame(this.spliterator, spliterator);
  }

  @Test
  public void isParallel() {
    boolean parallel = this.parallelStreamSupport.isParallel();

    verify(this.delegate).isParallel();
    assertTrue(parallel);
  }

  @Test
  public void sequential() {
    Stream<String> stream = this.parallelStreamSupport.sequential();

    verify(this.delegate).sequential();
    assertSame(this.parallelStreamSupport, stream);
  }

  @Test
  public void parallel() {
    Stream<String> stream = this.parallelStreamSupport.parallel();

    verify(this.delegate).parallel();
    assertSame(this.parallelStreamSupport, stream);
  }

  @Test
  public void unordered() {
    Stream<String> stream = this.parallelStreamSupport.unordered();

    verify(this.delegate).unordered();
    assertSame(this.parallelStreamSupport, stream);
  }

  @Test
  public void onClose() {
    Runnable r = () -> {};
    Stream<String> stream = this.parallelStreamSupport.onClose(r);

    verify(this.delegate).onClose(r);
    assertSame(this.parallelStreamSupport, stream);
  }

  @Test
  public void close() {
    this.parallelStreamSupport.close();

    verify(this.delegate).close();
  }

  @Test
  public void filter() {
    Predicate<String> p = s -> true;
    Stream<String> stream = this.parallelStreamSupport.filter(p);

    verify(this.delegate).filter(p);
    assertSame(this.parallelStreamSupport, stream);
  }

  @Test
  public void map() {
    Function<String, BigDecimal> f = s -> BigDecimal.ONE;
    Stream<BigDecimal> stream = this.parallelStreamSupport.map(f);

    verify(this.delegate).map(f);
    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertSame(ParallelStreamSupport.class.cast(stream).delegate, this.mappedDelegate);
  }

  @Test
  public void mapToInt() {
    ToIntFunction<String> f = s -> 1;
    IntStream stream = this.parallelStreamSupport.mapToInt(f);

    verify(this.delegate).mapToInt(f);
    assertThat(stream, instanceOf(ParallelIntStreamSupport.class));
    assertSame(ParallelIntStreamSupport.class.cast(stream).delegate, this.mappedIntDelegate);
  }

  @Test
  public void mapToLong() {
    ToLongFunction<String> f = s -> 1L;
    LongStream stream = this.parallelStreamSupport.mapToLong(f);

    verify(this.delegate).mapToLong(f);
    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertSame(ParallelLongStreamSupport.class.cast(stream).delegate, this.mappedLongDelegate);
  }

  @Test
  public void mapToDouble() {
    ToDoubleFunction<String> f = s -> 1D;
    DoubleStream stream = this.parallelStreamSupport.mapToDouble(f);

    verify(this.delegate).mapToDouble(f);
    assertThat(stream, instanceOf(ParallelDoubleStreamSupport.class));
    assertSame(ParallelDoubleStreamSupport.class.cast(stream).delegate, this.mappedDoubleDelegate);
  }

  @Test
  public void flatMap() {
    Function<String, Stream<BigDecimal>> f = s -> Stream.of(BigDecimal.ONE);
    Stream<BigDecimal> stream = this.parallelStreamSupport.flatMap(f);

    verify(this.delegate).flatMap(f);
    assertThat(stream, instanceOf(ParallelStreamSupport.class));
    assertSame(ParallelStreamSupport.class.cast(stream).delegate, this.mappedDelegate);
  }


  @Test
  public void flatMapToInt() {
    Function<String, IntStream> f = s -> IntStream.of(1);
    IntStream stream = this.parallelStreamSupport.flatMapToInt(f);

    verify(this.delegate).flatMapToInt(f);
    assertThat(stream, instanceOf(ParallelIntStreamSupport.class));
    assertSame(ParallelIntStreamSupport.class.cast(stream).delegate, this.mappedIntDelegate);
  }

  @Test
  public void flatMapToLong() {
    Function<String, LongStream> f = s -> LongStream.of(1L);
    LongStream stream = this.parallelStreamSupport.flatMapToLong(f);

    verify(this.delegate).flatMapToLong(f);
    assertThat(stream, instanceOf(ParallelLongStreamSupport.class));
    assertSame(ParallelLongStreamSupport.class.cast(stream).delegate, this.mappedLongDelegate);
  }

  @Test
  public void flatMapToDouble() {
    Function<String, DoubleStream> f = s -> DoubleStream.of(1L);
    DoubleStream stream = this.parallelStreamSupport.flatMapToDouble(f);

    verify(this.delegate).flatMapToDouble(f);
    assertThat(stream, instanceOf(ParallelDoubleStreamSupport.class));
    assertSame(ParallelDoubleStreamSupport.class.cast(stream).delegate, this.mappedDoubleDelegate);
  }

  @Test
  public void distinct() {
    Stream<String> stream = this.parallelStreamSupport.distinct();

    verify(this.delegate).distinct();
    assertSame(this.parallelStreamSupport, stream);
  }

  @Test
  public void sorted() {
    Stream<String> stream = this.parallelStreamSupport.sorted();

    verify(this.delegate).sorted();
    assertSame(this.parallelStreamSupport, stream);
  }

  @Test
  public void sortedWithComparator() {
    Comparator<String> c = (s1, s2) -> 0;
    Stream<String> stream = this.parallelStreamSupport.sorted(c);

    verify(this.delegate).sorted(c);
    assertSame(this.parallelStreamSupport, stream);
  }

  @Test
  public void peek() {
    Consumer<String> c = s -> {};
    Stream<String> stream = this.parallelStreamSupport.peek(c);

    verify(this.delegate).peek(c);
    assertSame(this.parallelStreamSupport, stream);
  }

  @Test
  public void limit() {
    Stream<String> stream = this.parallelStreamSupport.limit(5);

    verify(this.delegate).limit(5);
    assertSame(this.parallelStreamSupport, stream);
  }

  @Test
  public void skip() {
    Stream<String> stream = this.parallelStreamSupport.skip(5);

    verify(this.delegate).skip(5);
    assertSame(this.parallelStreamSupport, stream);
  }
}
