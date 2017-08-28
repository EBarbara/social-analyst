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
from datetime import datetime

from nltk.tokenize.casual import TweetTokenizer
from nltk.corpus import stopwords


def correct_timestamp(timestamp):
    return datetime.strptime(timestamp, '%a %b %d %H:%M:%S +0000 %Y')


def tokenize_and_remove_garbage(text):
    tokenizer = TweetTokenizer(reduce_len=True)
    tokenized = tokenizer.tokenize(text)

    stopwords_list = stopwords.words('english') + list(string.punctuation)
    user_mentions = [word for word in tokenized if (word.startswith('@') and len(word) > 1)]
    web_links = [word for word in tokenized if word.startswith('https://t.co/')]

    reduced_tokens = [word for word in tokenized if word not in stopwords_list + user_mentions + web_links]

    return reduced_tokens


def run(tweet_stream):
    tokenized_stream = tweet_stream.map(lambda tweet: (tweet[0],
                                                       correct_timestamp(tweet[1]),
                                                       tweet[2],
                                                       tweet[3],
                                                       tweet[4],
                                                       tokenize_and_remove_garbage(tweet[4])))
    return tokenized_stream
