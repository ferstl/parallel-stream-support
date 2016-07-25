package com.github.ferstl.streams;

import java.util.Comparator;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static java.lang.Thread.currentThread;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


public class ParallelStreamSupportTest {

  private ForkJoinPool workerPool;

  private Stream<String> delegate;
  private ParallelStreamSupport<String> parallelStreamSupport;

  @Before
  @SuppressWarnings("unchecked")
  public void before() {
    // Precondition for all tests
    assertFalse("This test must not run in a ForkJoinPool", currentThread() instanceof ForkJoinWorkerThread);

    this.workerPool = new ForkJoinPool(1);
    this.delegate = mock(Stream.class);
    this.parallelStreamSupport = new ParallelStreamSupport<>(this.delegate, this.workerPool);
  }

  @After
  public void after() throws InterruptedException {
    this.workerPool.shutdown();
    this.workerPool.awaitTermination(1, TimeUnit.SECONDS);
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
  public void filter() {
    Predicate<String> p = s -> true;
    Stream<String> stream = this.parallelStreamSupport.filter(p);

    verify(this.delegate).filter(p);
    assertSame(this.parallelStreamSupport, stream);
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
