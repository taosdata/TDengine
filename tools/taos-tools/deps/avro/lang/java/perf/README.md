# Apache Avro™ Performance Test Harness

The Apache Avro performance test harness is powered by OpenJDK JMH. JMH is a Java harness for building, running, and analysing nano/micro/milli/macro benchmarks written in Java.

## Usage

```
usage: Perf [--help] [--mi <measurementIterations>] [--test <test>] [--wi <warmupIterations>]

```

### Test Suites

| Test Suite             | Package                        |
| ---------------------- |:------------------------------:|
| Native Type Tests      | org.apache.avro.perf.test.basic.*   |
| Generic Datum Tests    | org.apache.avro.perf.test.generic.* |
| Record Tests           | org.apache.avro.perf.test.record.*  |
| Reflection Datum Tests | org.apache.avro.perf.test.reflect.* |


### Examples

```
-- Run all tests in a package
Perf --test org.apache.avro.perf.test.reflect.* --mi 2 --wi 2

-- Run a specific test
Perf --test org.apache.avro.perf.test.basic.IntTest --mi 3 --wi 3

-- Run all tests (measurementIterations=3, warmupIterations=3)
Perf
```

## Testing Caveats

JMH makes several warm ups, iterations etc. to make sure the results are not completely random. The more runs you have, the more accurate the performance information.

For best results, not other applications should be running while it runs the benchmarks. If your computer is running other applications, these applications may take time from the CPU and give incorrect (lower) performance numbers.

From the JMH Framework:

> The performance results are just data. To gain reusable insights, you need to follow up on
> why the numbers are the way they are. Use profilers (see -prof, -lprof), design factorial
> experiments, perform baseline and negative tests that provide experimental control, make
> sure the benchmarking environment is safe on JVM/OS/HW level, ask for reviews from the
> domain experts. Do not assume the numbers tell you what you want them to tell.
