from pyspark.streaming.kafka import KafkaUtils


class TwitterStreamingModule:
    def __init__(self, streaming_context):
        self.streamingContext = streaming_context

    def run(self):
        kafkaStream = KafkaUtils.createStream(self.streamingContext, zkQuorum, groupId, topics)

