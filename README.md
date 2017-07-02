# Overview
This project is a simple experiment examining the role that
[Apache Avro](https://avro.apache.org/) in the context of two applications
that communicate via message passing.  We'll will simulate the message passing
by writing messages to disk and having tests reading those files.

One of Avro's strengths is that can handle many forward and backward
compatibility scenarios.  It can do so because each message is associated
with a schema that allows the Avro runtime to make decisions about how to
convert a payload into an object that the application understands.

In our test scenario we will have two applications, one that produces the
messages and one that consumes them.  Ideally both applications should be
using the same message structure but, in practice, that rarely happens.  The
applications get updated and released on their own schedules so it is important
to allow each application to deal with message format changes at their own pace.

Luckily, Avro does not require the producer and consumer to use the same
schema.  Although it is possible to embed a "pointer" to a schema inside
each message, we will assume that each application has a schema embedded
inside it and only uses that.  Over time, each application will embed different
revisions of the same schema.  Our experiment will cover the following
scenarios:

| Producer      | Consumer      | Notes                                                                                      |
| ------------- | ------------- | ------------------------------------------------------------------------------------------ |
| Version 1.0.0 | Version 1.0.0 |                                                                                            |  
| Version 1.0.0 | Version 1.1.0 | Adds additional field in a forwards compatible way                                         |
| Version 1.1.0 | Version 1.1.0 |                                                                                            |
| Version 1.1.0 | Version 1.2.0 | Splits the name field into two fields in a forwards compatible way                         | 
| Version 1.2.0 | Version 1.2.0 |                                                                                            |
| Version 1.2.0 | Version 1.3.0 | Adds complex types, such as arrays, maps and promotable types in a forwards compatible way | 
| Version 1.3.0 | Version 1.3.0 |                                                                                            |
| Version 1.3.0 | Version 1.4.0 | Promotes the types, eg. int to long in a forwards compatible way                           |
| Version 2.0.0 | Version 2.0.0 |                                                                                            |
| Version 1.4.0 | Version 2.0.0 | Removes one field and adds another one in a forwards incompatible way                      | 

The schema version uses [Semantic Versioning](http://semver.org/) to indicate
breaking and non-breaking changes.

## Definitions
* **Backward Compatibility** - the writer is using a newer schema than the reader 
* **Forward Compatibility** - the writer is using an older schema than the reader 
* **Backward Compatibility** - the reader is using an older schema than the writer 
* **Forward Compatibility** - the reader is using a newer schema than the writer 
 
# Prerequisites

* [JDK](http://www.oracle.com/technetwork/java/index.html) installed and working
* [Avro Tools](http://avro.apache.org/releases.html) downloaded into the project directory

# Building
Use `./gradlew` to execute the [Gradle](https://gradle.org/) build script.

# Installation
There is noting to install.

# Tips and Tricks

## Jackson'S Avro Support
Initial testing was done using [Jackson's Avro support](https://github.com/FasterXML/jackson-dataformats-binary/tree/master/avro)
but it was quickly found that it does not support default values which is required
to maintain forward compatibility.  For that reason, the test code has been removed.

## Avro Code Generation
The tests were written using Avro's optional code generation facilities.  Although
it is possible to use Avro in a less structured way, via untyped key-vales, it
is assumed that application developers would prefer to use typed structures.

## Avro Inconveniences
The generated Avro structures do not use native JVM strings and, instead, use either
a custom UTF-8 class or `java.lang.CharSequence`.  For this reason, the tests
contain conversions that you might find odd.

## How We Test
Each schema revision must live in its own module because the schema's namespace
must remain constant or the compatibility conversions will not be applied.
For example, changing the namespace from `org.kurron.avro.example` to
`org.kurron.avro.example.v100` would, in Avro's mind, create two separate entities
and it would not attempt a conversion.

We are counting on Gradle's current behavior of building the modules in the order
that they are defined in the `settings.gradle` file.  This is required because
the input of a test is the output file of the previous module.  For example,
the v130 test attempts to read the v120 file when testing forwards compatibility.

## Interesting Avro Features
1. You can rename a field via the `aliases` construct.
1. Providing a default value for a field, via the `default` construct, guarantees forward compatibility.
1. Fields of type `int`, `long`, `float`, `string` and `byte` can be changed in a compatible way.
1. Rich constructs, such as `records`, `array`, `maps`, `enums` and `unions` provide many possible structures.
1. Logical types, including `Date`, `Time` and `Duration` exist.
1. Batch processing is supported via files.
1. RPC messaging is also supported.

## Backwards Compatibility Testing
To be complete, we tested backward compatibility scenarios.  For this experiment, we had to switch away
from generated, type-safe object and used generic key-value maps instead.

| Producer      | Consumer      | Notes                                                                                      |
| ------------- | ------------- | ------------------------------------------------------------------------------------------ |
| Version 1.1.0 | Version 1.0.0 |                                                                                            |  
| Version 1.2.0 | Version 1.0.0 |                                                                                            |
| Version 1.2.0 | Version 1.1.0 |                                                                                            |
| Version 1.3.0 | Version 1.0.0 |                                                                                            |
| Version 1.3.0 | Version 1.1.0 |                                                                                            |
| Version 1.3.0 | Version 1.2.0 |                                                                                            |
| Version 1.4.0 | Version 1.0.0 |                                                                                            |
| Version 1.4.0 | Version 1.1.0 |                                                                                            |
| Version 1.4.0 | Version 1.2.0 |                                                                                            |
| Version 1.4.0 | Version 1.3.0 |                                                                                            |

# Troubleshooting

# License and Credits
This project is licensed under the [Apache License Version 2.0, January 2004](http://www.apache.org/licenses/).
