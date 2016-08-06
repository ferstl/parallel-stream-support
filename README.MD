# Parallel Stream Support
*- Parallel streams in Java with a custom ForkJoinPool*

[![Build Status](https://travis-ci.org/ferstl/parallel-stream-support.svg?branch=master)](https://travis-ci.org/ferstl/parallel-stream-support) [![Coverage Status](https://coveralls.io/repos/github/ferstl/parallel-stream-support/badge.svg?branch=master)](https://coveralls.io/github/ferstl/parallel-stream-support?branch=master)

Parallel streams in Java 8 are by default processed in the [common pool](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ForkJoinPool.html#commonPool--). This is fine for many cases but sometimes it is not useful since there is no control over what else is running in the common pool. For example, an external library might use the common pool for IO-intensive tasks and therefore prevent the performance critical parallel streams of your application from executing. The stream API in Java 8 does not offer a mechanism to process parallel streams in a user-defined thread pool.

This library closes the gap and offers parallel streams in dedicated [ForkJoinPools](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ForkJoinPool.html). Like the standard stream API this library offers streams for Java objects and for `int`, `long` and `double` primitive types.

# How to Use
## Dependencies

The *Parallel Stream Support* library is available on [Maven Central](http://central.maven.org/maven2/com/github/ferstl/parallel-stream-support/). So no further repository configuration is required.

    <dependencies>
      <dependency>
        <groupId>com.github.ferstl</groupId>
        <artifactId>parallel-stream-support</artifactId>
        <version>1.0.0</version>
      </dependency>
    </dependencies>

## Creating Parallel Streams
To create a parallel stream you need to instantiate a ForkJoinPool and call one of the static factory methods of ParallelStreamSupport, ParallelIntStreamSupport, ParallelLongStreamSupport or ParallelDoubleStreamSupport. The factory methods are based on:

- The static factory methods of the [Stream](https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html) interface or its primitive variants
- The static factory methods of [StreamSupport](https://docs.oracle.com/javase/8/docs/api/java/util/stream/StreamSupport.html)
- [Collection.parallelStream()](https://docs.oracle.com/javase/8/docs/api/java/util/Collection.html#parallelStream--)
- [Arrays.stream()](https://docs.oracle.com/javase/8/docs/api/java/util/Arrays.html#stream-T:A-)

Take a look at the Javadoc for a complete overview of the factory methods.

**Example 1: Calculate the average weight of Elements**

    public double getAverageWeight(Collection<Element> elements) {
      ForkJoinPool pool = new ForkJoinPool(NR_OF_THREADS);

      double averageWeight = ParallelStreamSupport.parallelStream(elements, pool)
        .filter(Element::isAvailable)
        .mapToDouble(Element::getWeight)
        .average()
        .orElse(0.0);
        
        return averageWeight;
    }
    
**Example 2: Find a particular number in a random Array**

    public void find42(int[] randomArray) {
      ForkJoinPool pool = new ForkJoinPool();

      ParallelIntStreamSupport.parallelStream(randomArray, pool)
          .filter(i -> i == 42)
          .findAny()
          .orElseThrow(IllegalStateException::new);
    }

#FAQ
Q: How does it work?

A: The mechanism is pretty simple. A terminal operation on a parallel stream will recognize if it is executed within a ForkJoinPool. If it is not, the execution will be handed over to the common pool. If the operation was already started in a ForkJoinPool it will use that pool to complete the operation. This library utilizes this behavior and makes sure that terminal operations are started as task in the user-defined ForkJoinPool.

-----

Q: Does this library also support sequential streams?

A: Yes. Just call `sequential()` on the stream and it will be processed within the calling thread. When created, all streams of this library are configured to be parallel.

-----

Q: Why is there no public constructor which takes a regular Java 8 stream and a ForkJoinPool?

A: It is a strong principle of this library to hide the details of its implementation. So the only place you have to deal with a concrete class of this library is when a new parallel stream is created. Afterwards you will only see the standard interfaces Stream, IntStream, LongStream and DoubleStream. Furthermore, each stream variant offers similar factory methods as in [StreamSupport](https://docs.oracle.com/javase/8/docs/api/java/util/stream/StreamSupport.html), which allows you to create a parallel stream from any other stream by just calling [Stream.spliterator()](https://docs.oracle.com/javase/8/docs/api/java/util/stream/BaseStream.html#spliterator--).
Should you still think an essential factory method is missing, please report an [issue](https://github.com/ferstl/parallel-stream-support/issues) on this project.

# How to Build

    # Normal build with Javadoc and tests
    mvn clean install -Pgenerate-javadoc
    
    # Release
    mvn release:preapare release:perform

    # Coverage and publish to Coveralls
    # (reopToken not required on Travis)
    mvn clean jacoco:prepare-agent test jacoco:report coveralls:report -DrepoToken=<the-secret-token>
