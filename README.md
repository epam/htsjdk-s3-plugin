Amazon S3 plugin for HTSJDK
============================

# Overview
This is a plugin for [HTSJDK] [1]. This plugin enables multiconnection loading
of SAM/BAM files stored in AWS S3.  The plugin provides implementation of a 
custom reader that can be plugged into HTSJDK-based tools. The plugin does not 
add any new dependencies to HTSJDK; it is loadable and provides new functionality 
at runtime. A guide for benchmarking using public S3 BAM file can be found at 
[performance_test/public/README.md](performance_test/public/README.md).

The plugin requires Java 1.8, [HTSJDK] [1] v.2.1.1 or newer.

Plugin version: 1.0.

# Build and Usage


To build this package, use the following command:
~~~~
./gradlew shadowJar
~~~~

This command produces one file: `s3HtsjdkReaderFactory.jar`. 


To use this plugin with HTSJDK, use the custom reader factory by 
adding:
~~~~
-Dsamjdk.custom_reader=$PREFIX,$CLASS_NAME,$PLUGIN_PATH
~~~~
where
~~~~
CLASS_NAME=com.epam.cmbi.s3.S3ReaderFactory
PLUGIN_PATH=[path to S3HtsjdkRaderFactory.jar]
BUCKET=[S3 bucket]
KEY=[S3 key]
PREFIX=[Prefix of your s3 resources URLs (for example for url=http://s3.amazonaws.com/3kricegenome/9311/IRIS_313-15896.realigned.bam prefix would be 'http://s3.amazonaws.com/' or 'http://s3' etc.)]
~~~~

## Example: Working with Picard tools
The plugin is suitable for working with [Picard tools] [2] (tested with 
Picard-tools v.2.0.1) (you need to download and build the tools separately, see 
instructions [here] [3]).

It should be possible to run the Picard tools in the following way:
~~~~
CLASS_NAME=com.epam.cmbi.s3.S3ReaderFactory
PLUGIN_PATH=[path to S3HtsjdkRaderFactory.jar]
PICARD_JAR=[path to picard-tools.jar]
METRIC_COMMAND=[metrics with parameters]
BUCKET=[S3 bucket]
KEY=[S3 key]
PREFIX=http://s3.amazonaws.com/
java -Dsamjdk.custom_reader=$PREFIX,$CLASS_NAME,$PLUGIN_PATH -jar $PICARD_JAR $METRIC_COMMAND

For example:
java -Dsamjdk.custom_reader=http://s3.amazonaws.com,com.epam.cmbi.s3.S3ReaderFactory,/path/to/plugin/S3ReaderFactory.jar -jar /path/to/picard/picard.jar ViewSam INPUT=http://s3.amazonaws.com/3kricegenome/9311/IRIS_313-15896.realigned.bam VERBOSITY=INFO VALIDATION_STRINGENCY=SILENT
~~~~

### Interval list
Some of the Picard Tools metrics (e. g. ViewSam) take a list of intervals from 
an input file. When an index file is available for the BAM file and an interval 
list file is provided, plugin downloads only these intervals.


# AWS Authentication
In order to use the plugin, you need to set up AWS credentials. Credentials
must be set in at least one of the following locations:

1. `credentials` file at the following path: `[USER_HOME]/.aws/`. 
This file should contain the following lines in the order they appear here:

~~~~
[default]
aws_access_key_id=your_access_key_id
aws_secret_access_key=your_secret_access_key
~~~~

2. Environment variables: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_KEY`

# Configuration parameters
The plugin has the following configuration parameters (set using JVM options):

    * Number of connections to S3
      * JVM option `samjdk.s3plugin.number_of_connections`
      * Default value: 50

    The download process starts with the size of the partition equal to  
    equal to min_download_chunk_size. The partition size increases up to 
    max_download_chunk_size
    * Min download chunk size
      * JVM option `samjdk.s3plugin.min_download_chunk_size`
      * Default value: 32768 bytes = 32 kilobytes
    * Max download chunk size
      * JVM option `samjdk.s3plugin.max_download_chunk_size`
      * Default value: 8388608 bytes = 8 megabytes

    * Number of connection retries
      * JVM option `samjdk.s3plugin.custom_retry_count`
      * Default value: 3

    * Index file URL
      * JVM option `samjdk.s3plugin.index_file_url`
      * Default: try to find an index file using the 
      name of the BAM file (`.bai` or `.bam.bai` extention)

These options can be set using `-D$OPTION=$VALUE` syntax.

# Memory Usage
Theoretical upper memory requirement is calculated using the  following formula:

Theoretical upper memory requirement = max chunk size * number of connections * 3 (capacity buffer coefficient).

Default value equals 8MB * 50 *  3 = 1200 MB;

# Performance Monitoring
The plugin continuously reports on the amount of downloaded data, the number of GET 
requests to AWS S3 services and elapsed time. This information is written to the 
log every 5 seconds.

# Index files
Index files act as an external table of contents and allow the program to 
jump directly to specific parts of the BAM file without reading all of the 
sequence.

The plugin has the following two options for the index file location:
* The index file location is specified by the user in configuration parameters;
* The index file location is guessed from the BAM file name (if it is not 
specified in the configuration parameters.)

In the latter case, the index file is assumed to have the same name as the BAM 
file. First, the plugin looks for the `.bam.bai` file and if it does not exist,
the plugin searches for the `.bai` files. If the index file location was provided 
using the JVM option but its URL is wrong then the `IllegalArgumentException` 
exception is thrown.

# Downloading files from AWS S3
The plugin uses AWS Java SDK for downloading files from Amazon S3. The 
[`AmazonS3.getObject(GetObjectRequest)`][4] method is used for retrieving 
[`S3Object`][5] object which provides [`S3ObjectInputStream`][6]. The
`getObject` method uses [ObjectGET][7] service of the S3 REST API.

To download the index file, use a single GET request and a single
connection.

The BAM files are downloaded using multiple threads and thus retrieve the data 
in chunks of configurable size.


The target BAM file is partitioned to chunks while being downloaded. 

The chunk size of each downloading thread grows exponentially from 
`samjdk.s3plugin.min_download_chunk_size` to 
`samjdk.s3plugin.max_download_chunk_size`.
It is measured in bytes.

Then number of connections that the plugin creates is 
`samjdk.s3plugin.number_of_connections`. Every connection is 
processed as a separate task and creates a range request for a 
single chunk. The total number of GET requests for a file equals the total 
number of chunks. These tasks are put into a queue from which we get results in order to read the data.


## Reconnection
The plugin has the ability to reconnect to the server while downloading in case
the connection is lost. It is possible to configure the number of times the 
plugin tries to reconnect, using the `samjdk.s3plugin.custom_retry_count` 
parameter. Each time it tries to reconnect, the S3 Client makes 10 attempts, one 
in every 10 seconds.

It might end up downloading a bit of unused data if there are many reconnections 
due to the chunks downloading algorithm.
We downloaded about 1GB more with a 325GB BAM file when testing. This represents
an overhead of about 0.3%.


[1]: http://github.com/samtools/htsjdk
[2]: https://github.com/broadinstitute/picard
[3]: http://broadinstitute.github.io/picard/
[4]: http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3.html#getObject(com.amazonaws.services.s3.model.GetObjectRequest)
[5]: http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/S3Object.html
[6]: http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/S3ObjectInputStream.html
[7]: http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html