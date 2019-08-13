# Parallel Stream Support
*- Parallel streams in Java with a custom ForkJoinPool*

[![Build Status](https://travis-ci.org/ferstl/parallel-stream-support.svg?branch=master)](https://travis-ci.org/ferstl/parallel-stream-support) [![Coverage Status](https://coveralls.io/repos/github/ferstl/parallel-stream-support/badge.svg?branch=master)](https://coveralls.io/github/ferstl/parallel-stream-support?branch=master) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.ferstl/parallel-stream-support/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.ferstl/parallel-stream-support) [![Javadocs](http://javadoc.io/badge/com.github.ferstl/parallel-stream-support.svg?color=blue)](http://javadoc.io/doc/com.github.ferstl/parallel-stream-support) [![license](https://img.shields.io/github/license/mashape/apistatus.svg?maxAge=2592000)](https://opensource.org/licenses/MIT)

Parallel streams in Java 8 are by default processed in the [common pool](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ForkJoinPool.html#commonPool--). This is fine for many cases but sometimes it is not useful since there is no control over what else is running in the common pool. For example, an external library might use the common pool for IO-intensive tasks and therefore prevent the performance critical parallel streams of your application from executing. The stream API in Java 8 does not offer a mechanism to process parallel streams in a user-defined thread pool.

This library works around this gap and offers parallel streams in dedicated [ForkJoinPools](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ForkJoinPool.html). Like the standard stream API this library offers streams for Java objects and for `int`, `long` and `double` primitive types.

:warning: Having parallel streams outside of the common pool is not an officially supported feature and might stop working or behave differently in later Java releases! Furthermore the splitting policy of Parallel Streams depends strongly on the configured parallelism of the common pool. See this [StackOverflow Answer](http://stackoverflow.com/a/29272776/1497059) from Stuart Marks for further details:

> In the [java.util.stream.AbstractTask](http://hg.openjdk.java.net/jdk8/jdk8/jdk/file/jdk8-b132/src/share/classes/java/util/stream/AbstractTask.java) class, the LEAF_TARGET field determines the amount of splitting that is done, which in turn determines the amount of parallelism that can be achieved. The value of this field is based on `ForkJoinPool.getCommonPoolParallelism()` which of course uses the parallelism of the common pool, not whatever pool happens to be running the tasks.

# How to Use
## Version Compatibility
Each major version of this library belongs to one or more major versions of the JDK:

| Library Version | JDK Version(s) | Comment |
| --- | --- | --- |
| 1.x.x | JDK 8 | Covers all methods of `Stream` and its primitive variants.  |
| 2.x.x | JDKs 9, 10, 11, 12 | Adds support for `takeWhile()` and `dropWhile()` | 

## Dependencies

The *Parallel Stream Support* library is available on [Maven Central](http://central.maven.org/maven2/com/github/ferstl/parallel-stream-support/). So no further repository configuration is required.

    <dependencies>
      <dependency>
        <groupId>com.github.ferstl</groupId>
        <artifactId>parallel-stream-support</artifactId>
        <version>1.0.1</version>
      </dependency>
    </dependencies>

## Creating Parallel Streams
To create a parallel stream you need to instantiate a [ForkJoinPool](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ForkJoinPool.html) and call one of the static factory methods of [ParallelStreamSupport](http://static.javadoc.io/com.github.ferstl/parallel-stream-support/1.0.0/com/github/ferstl/streams/ParallelStreamSupport.html), [ParallelIntStreamSupport](http://static.javadoc.io/com.github.ferstl/parallel-stream-support/1.0.0/com/github/ferstl/streams/ParallelIntStreamSupport.html), [ParallelLongStreamSupport](http://static.javadoc.io/com.github.ferstl/parallel-stream-support/1.0.0/com/github/ferstl/streams/ParallelLongStreamSupport.html) or [ParallelDoubleStreamSupport](http://static.javadoc.io/com.github.ferstl/parallel-stream-support/1.0.0/com/github/ferstl/streams/ParallelDoubleStreamSupport.html). The factory methods are based on:

- The static factory methods of the [Stream](https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html) interface or its primitive variants
- The static factory methods of [StreamSupport](https://docs.oracle.com/javase/8/docs/api/java/util/stream/StreamSupport.html)
- [Collection.parallelStream()](https://docs.oracle.com/javase/8/docs/api/java/util/Collection.html#parallelStream--)
- [Arrays.stream()](https://docs.oracle.com/javase/8/docs/api/java/util/Arrays.html#stream-T:A-)

Take a look at the [Javadoc](https://javadoc.io/doc/com.github.ferstl/parallel-stream-support/) for a complete overview of the factory methods.

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
