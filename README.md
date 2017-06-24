# Overview
This project is a simple experiment examining the role that
[Apache Avro](https://avro.apache.org/) in the context of two applications
that communicate via message passing.  We'll be using
[RabbitMQ](https://www.rabbitmq.com/) as our broker.  

One of Avro's strengths is that can handle many forward and backward
compatibility scenarios.  It can do so because the application and, possibly,
each message is associated with a schema that allows the Avro runtime to
make decisions about how to convert a payload into an object that the application
understands.

In our test scenario we will have two applications, one that produces the
messages and one that consumes them.  In theory, both applications should be
using the same message structure but in practice, that rarely happens.  The
applications get updated and released on their own schedules so it is important
to allow each application to deal with message format changes at their own pace.

Luckily, Avro does not require the producer and consumer to use the same
schema.  Although it is possible to embed a "pointer" to a schema inside
each message, we will assume that each application has a schema embedded
inside it and only uses that.  Over time, each application will embed different
revisions of the same schema.  Our experiment will cover the following
scenarios:

| Producer      | Consumer      |
| ------------- | ------------- |
| Version 1.0.0 | Version 0.0.0 |
| Version 1.1.0 | Version 0.0.0 |
| Version 1.2.0 | Version 0.0.0 |
| Version 1.3.0 | Version 0.0.0 |

| Version 1.0.0 | Version 1.1.0 |
| Version 2.0.0 | Version 1.0.0 |
| Version 1.0.0 | Version 2.0.0 |

The schema version uses [Semantic Versioning](http://semver.org/) to indicate
breaking and non-breaking changes.

## Definitions
* **Backward Compatibility** - the writer is using a newer schema than the reader 
* **Forward Compatibility** - the writer is using an older schema than the reader 
 
# Prerequisites

* [JDK](http://www.oracle.com/technetwork/java/index.html) installed and working
* [RabbitMQ](https://www.rabbitmq.com/) installed and working

# Building
Use `./gradlew` to execute the [Gradle](https://gradle.org/) build script.

# Installation
There is noting to install.

# Tips and Tricks

## Jackson'S Avro Support
Initial testing was done using [Jackson's Avro support](https://github.com/FasterXML/jackson-dataformats-binary/tree/master/avro)
but it was quickly found that it does not support default values.  The test code has
since been removed.

# Troubleshooting

# License and Credits
This project is licensed under the [Apache License Version 2.0, January 2004](http://www.apache.org/licenses/).
