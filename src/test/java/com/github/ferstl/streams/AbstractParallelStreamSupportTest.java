package com.github.ferstl.streams;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.BaseStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public abstract class AbstractParallelStreamSupportTest<T, S extends BaseStream<T, S>, R extends AbstractParallelStreamSupport<T, S>> {

  ForkJoinPool workerPool;
  S delegateMock;
  R parallelStreamSupportMock;

  protected abstract R createParallelStreamSupportMock(ForkJoinPool workerPool);

  @Before
  public void before() {
    this.workerPool = new ForkJoinPool(1);
    this.parallelStreamSupportMock = createParallelStreamSupportMock(this.workerPool);
    this.delegateMock = this.parallelStreamSupportMock.delegate;
  }

  @After
  public void after() throws InterruptedException {
    this.workerPool.shutdown();
    this.workerPool.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void iterator() {
    Iterator<?> iteratorMock = mock(Iterator.class);
    when(this.delegateMock.iterator()).thenReturn((Iterator) iteratorMock);
    Iterator<?> iterator = this.parallelStreamSupportMock.iterator();

    verify(this.delegateMock).iterator();
    assertSame(iteratorMock, iterator);
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void spliterator() {
    Spliterator<?> spliteratorMock = mock(Spliterator.class);
    when(this.delegateMock.spliterator()).thenReturn((Spliterator) spliteratorMock);
    Spliterator<?> spliterator = this.parallelStreamSupportMock.spliterator();

    verify(this.delegateMock).spliterator();
    assertSame(spliteratorMock, spliterator);
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
    BaseStream<?, ?> stream = this.parallelStreamSupportMock.sequential();

    verify(this.delegateMock).sequential();
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void parallel() {
    BaseStream<?, ?> stream = this.parallelStreamSupportMock.parallel();

    verify(this.delegateMock).parallel();
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void unordered() {
    BaseStream<?, ?> stream = this.parallelStreamSupportMock.unordered();

    verify(this.delegateMock).unordered();
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void onClose() {
    Runnable r = () -> {};
    BaseStream<?, ?> stream = this.parallelStreamSupportMock.onClose(r);

    verify(this.delegateMock).onClose(r);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void close() {
    this.parallelStreamSupportMock.close();

    verify(this.delegateMock).close();
  }
}
