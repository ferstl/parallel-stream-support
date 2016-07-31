package com.github.ferstl.streams;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.BaseStream;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class AbstractParallelStreamSupportTest {

  private ForkJoinPool workerPool;

  private Stream<String> delegateMock;
  private Iterator<?> iteratorMock;
  private Spliterator<?> spliteratorMock;

  private AbstractParallelStreamSupport<String, Stream<String>> parallelStreamSupportMock;

  @Before
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void before() {
    this.workerPool = new ForkJoinPool(1);
    this.delegateMock = mock(Stream.class);
    this.iteratorMock = mock(Iterator.class);
    this.spliteratorMock = mock(Spliterator.class);

    when(this.delegateMock.iterator()).thenReturn((Iterator) this.iteratorMock);
    when(this.delegateMock.spliterator()).thenReturn((Spliterator) this.spliteratorMock);

    this.parallelStreamSupportMock = new AbstractParallelStreamSupport<String, Stream<String>>(this.delegateMock, this.workerPool) {};
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
    BaseStream<String, Stream<String>> stream = this.parallelStreamSupportMock.sequential();

    verify(this.delegateMock).sequential();
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void parallel() {
    BaseStream<String, Stream<String>> stream = this.parallelStreamSupportMock.parallel();

    verify(this.delegateMock).parallel();
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void unordered() {
    BaseStream<String, Stream<String>> stream = this.parallelStreamSupportMock.unordered();

    verify(this.delegateMock).unordered();
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void onClose() {
    Runnable r = () -> {};
    BaseStream<String, Stream<String>> stream = this.parallelStreamSupportMock.onClose(r);

    verify(this.delegateMock).onClose(r);
    assertSame(this.parallelStreamSupportMock, stream);
  }

  @Test
  public void close() {
    this.parallelStreamSupportMock.close();

    verify(this.delegateMock).close();
  }
}
