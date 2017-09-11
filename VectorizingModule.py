from pyspark.ml.feature import HashingTF


class VectorizingModule:
    def __init__(self, inputCol, outputCol):
        self.hashing_tf = HashingTF(numFeatures=5000, inputCol=inputCol, outputCol=outputCol)

    def run(self, data_frame):
        vectorized_data = self.hashing_tf.transform(data_frame)
        return vectorized_data
