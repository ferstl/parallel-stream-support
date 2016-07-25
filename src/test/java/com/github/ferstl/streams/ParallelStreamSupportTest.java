package com.github.ferstl.streams;

import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static java.lang.Thread.currentThread;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;


public class ParallelStreamSupportTest {

  private static final Pattern FORK_JOIN_THREAD_NAME_PATTERN = Pattern.compile("ForkJoinPool-\\d+-worker-\\d+");

  private ForkJoinPool workerPool;

  @Before
  public void before() {
    // Precondition for all tests
    assertFalse("This test must not run in a ForkJoinPool", currentThread() instanceof ForkJoinWorkerThread);

    this.workerPool = new ForkJoinPool(1);
  }

  @After
  public void after() throws InterruptedException {
    this.workerPool.shutdown();
    this.workerPool.awaitTermination(1, TimeUnit.SECONDS);
  }


  @Test
  public void collectParallel() {
    List<String> result = ParallelStreamSupport.parallelStream(singletonList("a"), this.workerPool)
        .map(e -> Thread.currentThread().getName())
        .collect(toList());

    assertThat(result, contains(matchesPattern(FORK_JOIN_THREAD_NAME_PATTERN)));
  }
}
