# Managed to work with Structured Streaming
'''from pyspark.ml.feature import StopWordsRemover, Tokenizer

class PreprocessingModule:
     def __init__(self, inputCol, outputCol):
         self.tokenizer = Tokenizer(inputCol=inputCol, outputCol="tokens")
         self.stopword_remover = StopWordsRemover(inputCol="tokens", outputCol=outputCol)

     def run(self, dataFrame):
         tokenized = self.tokenizer.transform(dataFrame)
         filtered = self.stopword_remover.transform(tokenized)
         return filtered
'''
import string

from nltk import word_tokenize
from nltk.corpus import stopwords


def tokenize_and_filter(text):
    stopwords_list = stopwords.words('english') + list(string.punctuation)
    tokenized = word_tokenize(text.lower())
    filtered = [i for i in tokenized if i not in stopwords_list]
    return filtered


def run(tweet_stream):
    filtered_stream = tweet_stream.map(lambda tweet: (tweet[0],
                                                      tweet[1],
                                                      tweet[2],
                                                      tweet[3],
                                                      tweet[4],
                                                      tokenize_and_filter(tweet[4])))

    return filtered_stream