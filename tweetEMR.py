from __future__ import print_function

import sys
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

if __name__ == "__main__":
    applicationName = "PythonStreamingKinesisWordCountAsl"
    streamName= "KinesisDemo"
    endpointUrl="https://kinesis.us-east-1.amazonaws.com"
    regionName="us-east-1"
    sc = SparkContext(appName=applicationName)
    ssc = StreamingContext(sc, 5)

    print("appname is" + applicationName + streamName + endpointUrl + regionName)
    lines = KinesisUtils.createStream(ssc, applicationName, streamName, endpointUrl, regionName, InitialPositionInStream.LATEST, 2)

    lines.pprint()

    def format_sample(x):
        data = json.loads(x)
        return (data['id'], json.dumps(data))

    parsed = lines.map(lambda x: format_sample(x))
    parsed.pprint()

    ssc.start()
    ssc.awaitTermination()