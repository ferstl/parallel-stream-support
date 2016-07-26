package com.github.ferstl.streams;

import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.ForkJoinTask.adapt;

abstract class AbstractParallelStreamSupport<T> {

  T delegate;
  final ForkJoinPool workerPool;

  AbstractParallelStreamSupport(T delegate, ForkJoinPool workerPool) {
    requireNonNull(delegate, "Stream must not be null");
    requireNonNull(workerPool, "Stream must not be null");

    this.delegate = delegate;
    this.workerPool = workerPool;
  }

  protected abstract boolean isParallel();

  protected void execute(Runnable terminalOperation) {
    if (isParallel()) {
      ForkJoinTask<?> task = adapt(terminalOperation);
      this.workerPool.invoke(task);
    } else {
      terminalOperation.run();
    }
  }

  protected <R> R execute(Callable<R> terminalOperation) {
    if (isParallel()) {
      ForkJoinTask<R> task = adapt(terminalOperation);
      return this.workerPool.invoke(task);
    }

    try {
      return terminalOperation.call();
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
