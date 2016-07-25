package com.github.ferstl.streams;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
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
}
