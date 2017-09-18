import csv
import html
import json
import os
import socket
import sys
from datetime import datetime
from os.path import join, dirname

import tweepy
from dotenv import load_dotenv
from tweepy import StreamListener, Stream

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

try:
    CONSUMER_KEY = os.environ.get('TWITTER_CONSUMER_KEY')
    CONSUMER_SECRET = os.environ.get('TWITTER_CONSUMER_SECRET')
    ACCESS_TOKEN = os.environ.get('TWITTER_ACCESS_TOKEN')
    ACCESS_SECRET = os.environ.get('TWITTER_ACCESS_SECRET')
except KeyError:
    sys.stderr.write("TWITTER_* environment variables not set\n")
    sys.exit(1)

northeast_usa = [-74.44, 40.23, -73.44, 41.23]
united_kingdom = [-10.85, 49.82, 2.02, 59.48]
grande_rio = [-23.08, -43.79, -22.77, -43.13]
brasil = [-75.11, -53.35, 5.70, -33.92]
usa = [24.39, -124.85, 49.39, -66.88]


def extract_coordinates(coordinate_data, location_data):
    try:
        if coordinate_data is None:
            bounding_box = location_data['bounding_box']['coordinates'][0]
            lon_sum = 0.0
            lat_sum = 0.0
            for coordinates in bounding_box:
                lon_sum += coordinates[0]
                lat_sum += coordinates[1]
            coord = [lon_sum / 4, lat_sum / 4]
        else:
            coord = coordinate_data['coordinates']
    except TypeError:
        coord = [0, 0]
    return coord


def preprocessing(tweet_json):
    id = tweet_json['id']
    time = datetime.strptime(tweet_json['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
    coordinates = extract_coordinates(tweet_json["coordinates"], tweet_json["place"])
    longitude = coordinates[0]
    latitude = coordinates[1]
    text = tweet_json["text"].replace('\n', ' ')
    return id, time, latitude, longitude, text


class StdOutListener(StreamListener):
    def __init__(self, use_socket=False):
        self.use_socket = use_socket
        self.socket = None
        TCP_IP = "localhost"
        TCP_PORT = 9999
        if self.use_socket:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.bind((TCP_IP, TCP_PORT))
            self.socket.listen(1)
            self.socket.accept()

    def on_data(self, data):
        if self.use_socket:
            tweet_json = json.loads(html.unescape(data))
            tweet = preprocessing(tweet_json)
            self.socket.send((str(tweet) + '\n').encode('UTF-8'))
        else:
            try:
                tweet_json = json.loads(html.unescape(data))
                tweet = preprocessing(tweet_json)
                tweet_id = str(tweet[0])
                with open("tweets/tweet.csv", 'a', encoding='utf-8') as csv_file:
                # with open("tweets/tweet" + tweet_id + ".csv", 'a', encoding='utf-8') as csv_file:
                    field_names = ['id', 'time', 'latitude', 'longitude', 'text']
                    writer = csv.DictWriter(csv_file, delimiter=';', lineterminator='\n', fieldnames=field_names)
                    writer.writerow({'id': tweet[0],
                                     'time': tweet[1],
                                     'latitude': tweet[2],
                                     'longitude': tweet[3],
                                     'text': tweet[4]})

            except KeyError:
                pass
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    listener = StdOutListener(False)
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    stream = Stream(auth, listener)
    stream.filter(languages=['en'], locations=northeast_usa + united_kingdom)
