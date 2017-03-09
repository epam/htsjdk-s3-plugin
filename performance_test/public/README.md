# Public benchmark

Benchmark.sh runs a pre-configured benchmark on a public s3-located BAM-file.

In order to run it, you should build the plugin jar file.
If you're in the plugin root folder, change the directory to S3HtsjdkPlugin by typing

`cd S3HtsjdkPlugin`

then build the plugin jar by typing

`../gradlew shadowJar`

After you've done that, change the directory by typing

`cd ../performance_test/public`

and then type

`sh benchmark.sh`

to run the script.

This benchmark downloads an 2.2Gb(!) BAM file two times, first using our plugin,
then using default picard implementation. Then it prints the elapsed time for both cases.

It is possible to set your own data source changing url in the script.