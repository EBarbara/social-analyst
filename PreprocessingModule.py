from pyspark.ml.feature import StopWordsRemover, Tokenizer


class PreprocessingModule:
    def __init__(self, inputCol, outputCol):
        self.tokenizer = Tokenizer(inputCol=inputCol, outputCol="tokens")
        self.stopword_remover = StopWordsRemover(inputCol="tokens", outputCol=outputCol)

    def run(self, dataFrame):
        tokenized = self.tokenizer.transform(dataFrame)
        filtered = self.stopword_remover.transform(tokenized)
        return filtered