
## Running with Hadoop

    ./bin/hadoop jar ./contrib/streaming/hadoop-streaming-1.2.1.jar \
      -input /input/features.csv \
      -output /output.15 \
      -mapper "gossamr -task 0 -phase map" \
      -io typedbytes \
      -file /path/to/bin \
      -numReduceTasks 6
