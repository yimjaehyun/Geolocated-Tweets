import tweepy
import json
import os
import sys
from pathlib import Path


TOTAL_SIZE = 5000   # in MB (5 GB)
SIZE_OF_FILES = 10  # in MB (10 MB)

auth = tweepy.OAuthHandler(os.environ['API_KEY'], os.environ['API_SECRET'])
#
# try:
#     redirect_url = auth.get_authorization_url()
# except tweepy.TweepError:
#     print 'Error! Failed to get request token.'
#
# print(redirect_url)
# verifier = raw_input('Verifier:')
#
# try:
#     auth.get_access_token(verifier)
# except tweepy.TweepError:
#     print 'Error! Failed to get access token.'
#
# print(auth.access_token)
# print(auth.access_token_secret)
#
# api = tweepy.API(auth)
auth.set_access_token(os.environ['ACCESS_KEY'], os.environ['ACCESS_SECRET'])
api = tweepy.API(auth)


#override tweepy.StreamListener to add logic to on_status
class MyStreamListener(tweepy.StreamListener):
    i = 1
    num_files = TOTAL_SIZE/SIZE_OF_FILES
    arr = []

    def on_status(self, status):
        print(status.text)

        json_status = json.loads(json.dumps(status._json))
        data = {}
        data['text'] = json_status['text']
        data['created_at'] = json_status['created_at']
        data['place'] = json_status['place']
        data['user'] = json_status['user']
        data['links'] = json_status['entities']

        file_name = 'tweet' + str(self.i) + '.json'
        file_path = Path('./tweets/' + file_name)

        if self.i == self.num_files:
            return False

        self.arr.append(data)
        output = json.dumps(self.arr, indent = 4)
        print(sys.getsizeof(output) / 1024)
        if (sys.getsizeof(output) / 1024) >= 10000:
            with open('./tweets/' + file_name, 'w') as outfile:
                json.dump(self.arr, outfile, indent=4)
            self.arr = []
            self.i = self.i+1

    def on_error(self, status_code):
        print("error: " + str(status_code))
        if status_code == 420:
            #returning False in on_data disconnects the stream
            return False

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)

#-33.9,-75.0,-9.7,85.0
#-122.75,36.8,-121.75,37.8,124.2,33.04,130.83,43.01,128.58,23.61,149.92,45.75
myStream.filter(locations=[-33.9,-75.0,-9.7,85.0], is_async=True)
