package com.github.ferstl;

import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class ParallelStreamSupportTest {

  private ForkJoinPool workerPool;

  @Before
  public void before() {
    this.workerPool = new ForkJoinPool(1);
  }

  @After
  public void after() throws InterruptedException {
    this.workerPool.shutdown();
    this.workerPool.awaitTermination(1, TimeUnit.SECONDS);
  }


  @Test
  public void test() {
    ParallelStreamSupport.parallelStream(Arrays.asList("a"), this.workerPool)
        .forEach(e -> System.out.println(Thread.currentThread().getName()));
  }

}
