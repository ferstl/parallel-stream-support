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

import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.BaseStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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

  @Test
  public void executeWithRunnable() {
    AtomicBoolean b = new AtomicBoolean(false);

    this.parallelStreamSupportMock.execute(() -> b.set(true));

    assertTrue(b.get());
  }

  @Test(expected = RuntimeException.class)
  public void executeWithRunnableThrowingException() {
    Runnable r = () -> {
      throw new RuntimeException("boom");
    };

    this.parallelStreamSupportMock.execute(r);
  }

  @Test
  public void executeWithCallable() {
    AtomicBoolean b = new AtomicBoolean(false);
    Callable<Void> c = () -> {
      b.set(true);
      return null;
    };

    this.parallelStreamSupportMock.execute(c);

    assertTrue(b.get());
  }

  @Test(expected = AssertionError.class)
  public void executeWithCallableThrowingError() {
    Callable<Void> c = () -> {
      throw new AssertionError("boom");
    };

    this.parallelStreamSupportMock.execute(c);
  }

  @Test
  public void executeWithCallableThrowingCheckedException() {
    Exception e = new Exception("boom");
    try {
      Callable<Void> c = () -> {
        throw e;
      };

      this.parallelStreamSupportMock.execute(c);
      fail("Expect runtime exception.");
    } catch (RuntimeException rte) {
      assertEquals(e, rte.getCause());
    }
  }
}
