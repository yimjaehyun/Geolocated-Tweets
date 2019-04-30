import tweepy
import json
import os
import sys
from pathlib import Path
from scrapy.crawler import CrawlerProcess
from urllib.request import urlopen
from bs4 import BeautifulSoup


TOTAL_SIZE = 5000   # in MB (5 GB)
SIZE_OF_FILES = 10  # in MB (10 MB)
links = []

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
        global links
        print(status.text)
        if status.place.full_name:
            print(status.place.full_name)

        json_status = (status._json)
        # data = {}
        # data['text'] = json_status['text']
        # data['created_at'] = json_status['created_at']
        # data['place'] = json_status['place']['full_name']
        # data['user'] = json_status['user']['screen_name']
        # links_length = len(json_status['entities']['urls'])
        # data['links'] = []
        # if links_length > 0:
        #     for i in range(links_length):
        #         data['links'].append(json_status['entities']['urls'][i]['expanded_url'])

        for i in range(len(json_status['entities']['urls'])):
            url = str(json_status['entities']['urls'][i]['expanded_url'])
            soup = BeautifulSoup(urlopen(url), features="lxml")
            json_status['entities']['urls'][i]['title'] = soup.title.string
            print(url + "\t" + soup.title.string)
            # links.append(str(json_status['entities']['urls'][i]['expanded_url']))

        file_name = 'tweet' + str(self.i) + '.json'
        file_path = Path('./tweets/' + file_name)

        if self.i == self.num_files:
            return False

        self.arr.append(json_status)
        output = json.dumps(self.arr)
        print(sys.getsizeof(output) / 1024)
        if (sys.getsizeof(output) / 1024) >= 10000: # 10000
            with open('./tweets/' + file_name, 'w') as outfile:
                json.dump(self.arr, outfile)
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
myStream.filter(locations=[-118.308216,34.019505,-118.189212,34.094909])
#
# for link in links:
#     soup = BeautifulSoup(urlopen(link))
#     print(soup.title.string)
