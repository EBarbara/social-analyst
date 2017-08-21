import csv
from collections import defaultdict
from math import sqrt
from nltk import word_tokenize
from pyspark.sql.functions import lit, array


def load_model(model_file):
    with open(model_file, "rb") as lines:
        gloVe = defaultdict(list)
        for line in lines:
            vector = line.split()
            element = vector[0].decode('UTF-8')
            iterator = iter(vector)
            next(iterator)  # skipping first entry (the word)
            for dimension in iterator:
                gloVe[element].append(float(dimension))
    return gloVe


class VectorizingModule:
    def __init__(self, model_file):
        self.model = load_model(model_file)

    def vectorize(self, text):
        tokens = word_tokenize(text)

        word_count = {}
        for token in tokens:
            if token not in word_count:
                word_count[token] = 1
            else:
                word_count[token] += 1

        word_vectors = defaultdict(list)
        for word in word_count:
            if word in self.model:
                for dimension in self.model[word]:
                    word_vectors[word].append(dimension * word_count[word])

        document_vector = []
        for i in range(0, 24):
            square_sum = 0.0
            for word in word_vectors:
                square_sum += word_vectors[word][i] ** 2
            document_vector.append(lit(sqrt(square_sum)))

        return array(document_vector)


if __name__ == "__main__":

    twitter_file = "C:\\Users\\Estevan\\PycharmProjects\\Mining\\tweets\\tweet899469608423493637.csv"
    tweet = {}
    with open(twitter_file) as csv_file:
        field_names = ['id', 'time', 'coordinates', 'location', 'text']
        reader = csv.DictReader(csv_file, delimiter=';', lineterminator='\n', fieldnames=field_names)
        for line in reader:
            tweet = line
    print(tweet)

    with open("glove.twitter.27B.25d.txt", "rb") as lines:
        gloVe = defaultdict(list)
        for line in lines:
            vector = line.split()
            element = vector[0].decode('UTF-8')
            iterator = iter(vector)
            next(iterator)  # skipping first entry (the word)
            for dimension in iterator:
                gloVe[element].append(float(dimension))

    print(vectorize(tweet["text"], gloVe)[0])
