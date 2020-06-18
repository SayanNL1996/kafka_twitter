import re
from sql_connection import conn, connection
import time
import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient

ACCESS_TOKEN = '1252139667710980096-GnpsQlFLrJ4LvWqwipPIC2ffC4wyog'
ACCESS_SECRET = 'VgNG5hYqzSoaAwNS1LVrlDENXBJoOzr2EPYjtTelPn1iF'
CONSUMER_KEY = 'PfkSyve5w0fAR7g50PSX4HiBc'
CONSUMER_SECRET = 'UlGZF549G3WA9VKUkk1fkh0CHg3O9tAUbnKFVCesPi78Zv8WwI'


class kafka_listener(StreamListener):

    def __init__(self):
        super().__init__()
        self.start_time = time.time()
        self.limit = 18000

    def on_data(self, data):
        if (time.time() - self.start_time) < self.limit:
            tweet_data = json.loads(data)

            name = tweet_data["user"]["screen_name"]
            tweet_time = time.strftime("%Y-%m-%d", time.localtime(int((tweet_data['timestamp_ms'])) / 1000))
            user_location = tweet_data["user"]["location"]
            fetch_location = "Not available" if user_location is None else user_location
            check_hashtag = tweet_data["entities"]["hashtags"]
            try:
                if check_hashtag != [] and fetch_location != "Not available":
                    hashtags = tweet_data["entities"]["hashtags"][0]["text"]
                    tweet_time = time.strftime('%Y-%m-%d %H:%M:%S',
                                               time.localtime(int((tweet_data['timestamp_ms'])) / 1000))
                    tweet = ' '.join(
                        re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])| (\w+:\ / \ / \S+)", " ", tweet_data["text"]).split())

                    print(hashtags)
                    print(name)
                    print(tweet)
                    print(fetch_location)
                    print(tweet_time)

                    conn.execute(
                        'INSERT INTO tweet (time, username, tweet_data, location, hashtag) VALUES (%s,%s,%s,%s,%s)',
                        (str(tweet_time), name, tweet, str(fetch_location), str(hashtags)))
                    connection.commit()

                    producer.send_messages("stream", data.encode('utf-8'))
                    print(data)
                    return True
                return True
            except Exception as e:
                print(e)
                return True
        return False

    def on_error(self, status):
        print(status)


kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
listen = kafka_listener()
auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
stream = Stream(auth, listen)
stream.filter(track="corona")
