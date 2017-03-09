#!/bin/sh
test_name="Compare Picard tools standart with usage of S3 HTSJDK plugin"

echo $test_name
prefix=http://s3.amazonaws.com/

if [ $# -gt 0 ];then
	url=$1
else
	url=http://s3.amazonaws.com/3kricegenome/9311/IRIS_313-15896.realigned.bam
fi

wget https://github.com/broadinstitute/picard/releases/download/2.8.1/picard.jar

picard_path=$(pwd)/picard.jar

if [ $(expr substr $(uname -s) 1 5) = "MINGW" ]; then
	driver_letter=$(echo $picard_path | cut -f2 -d"/"):
	picard_path=$driver_letter$(echo $picard_path | cut -c 3-)
fi


plugin_path=$(pwd)/../../S3HtsjdkPlugin/build/libs/s3HtsjdkReaderFactory.jar

if [ $(expr substr $(uname -s) 1 5) = "MINGW" ]; then
	driver_letter=$(echo $plugin_path | cut -f2 -d"/"):
	plugin_path=$driver_letter$(echo $plugin_path | cut -c 3-)
fi

echo
echo "######################### S3 plugin download:"
echo 

START=$(date +%s)
java -jar -Dsamjdk.s3plugin.number_of_connections=50 -Dsamjdk.custom_reader=$prefix,com.epam.cmbi.s3.S3ReaderFactory,$plugin_path $picard_path ViewSam VERBOSITY=INFO VALIDATION_STRINGENCY=SILENT INPUT=$url > /dev/null
END=$(date +%s)
plugin_time=$(( $END - $START ))

echo 
echo "######################### Standard version:"
echo  
START=$(date +%s)
java -jar $picard_path ViewSam VERBOSITY=INFO VALIDATION_STRINGENCY=SILENT INPUT=$url > /dev/null
END=$(date +%s)
standard_time=$(( $END - $START ))

rm picard.jar

echo '\n'
echo Elapsed time of standard Picard tools: $standard_time sec
echo Elapsed time of Picard tools with S3 HTSJDK plugin: $plugin_time sec
