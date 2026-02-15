pytest-kafka-broker documentation
=================================

This is a pytest plugin to run a temporary, local, single-broker Kafka cluster.

You must have Java installed. Other than that, there are no dependencies. The
plugin will download and cache Kafka and start and stop it automatically when
you use the fixture.

*******
Example
*******

.. literalinclude:: ../tests/test_kafka.py

*********
Reference
*********

.. automodapi:: pytest_kafka_broker
    :no-inheritance-diagram:

***********************
Known Issues on Windows
***********************

This plugin includes some workarounds for a few quirks on Windows that would otherwise make it difficult to run a Kafka broker:

* Long values of :envvar:`CLASSPATH` `run up against command-line length limits <https://stackoverflow.com/questions/48834927/the-input-line-is-too-long-when-starting-kafka>`_. As a workaround, the plugin temporarily maps the :envvar:`KAFKA_HOME` directory to a drive letter.

* The Windows version of the :program:`kafka-server-start` script uses the :program:`wmic` command to determine an appropriate value ofr the Java heap size, but this command was removed in recent versions of Windows. This is a `known bug in Kafka <https://issues.apache.org/jira/browse/KAFKA-19890>`_. As a workaround, the plugin set the environment variable :envvar:`KAFKA_HEAP_OPTS` to an explicit value.
