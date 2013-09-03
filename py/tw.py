import tweepy
import ConfigParser
import argparse
import redis
import csv
from datetime import datetime
import pandas as pd
import numpy as np

REDIS_DB = 1
REDIS_PORT = 6379
REDIS_HOST = 'localhost'
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)


def build_que(api, search_term):
    """
    Builds a que of stories/tweets
    """
    res = api.search(search_term, 'en')
    q = {}
    for i in res:
        q[i.id_str] = {'name': i.user.name,
                       'screen_name': i.user.screen_name,
                       'tweet': i.text,
                       'fav_count': i.favorite_count,
                       'rtw_count': i.retweet_count,
                       'created_at': i.created_at,
                       'user_image_url': i.user.profile_image_url,
                       'user_follower_count': i.user.followers_count,
                       'q': search_term
                        }
    return q


def populate_unprocessed_que(res):
    """
    res: Results of latest query that will get loaed into Redis
    """
    for id in res:
        dt = str(datetime.now()).replace(' ', '').replace('-', '').replace('.', '').replace(':', '')
        values = {'name': res[id]['name'],
                  'screen_name': res[id]['screen_name'],
                  'tweet': res[id]['tweet'],
                  'fav_count': res[id]['fav_count'],
                  'rtw_count': res[id]['rtw_count'],
                  'created_at': res[id]['created_at'],
                  'user_image_url': res[id]['user_image_url'],
                  'user_follower_count': res[id]['user_follower_count'],
                  'q': res[id]['q']
                  }
        r.hmset("upt:%s:%s:%s" % (str(id), res[id]['screen_name'], dt), values)
        print "added: %s" % str(values)


if __name__ == '__main__':
    #set inputs from commandline
    parser = argparse.ArgumentParser(description='Search Term')
    parser.add_argument('-c', dest='conf', required=True, help='Config File')
    parser.add_argument('-p', dest='path', required=True, help='File with search terms')

    args = parser.parse_args()
    twcf = args.conf
    path = args.path

    #Process Configurator File
    config = ConfigParser.ConfigParser()
    config.read(twcf)

    consumer_key = config.get('API', 'consumer_key')
    consumer_secret = config.get('API', 'consumer_secret')
    access_token = config.get('API', 'access_token')
    access_token_secret = config.get('API', 'access_token_secret')

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)
    with open(path) as f:
        search_terms = f.read().splitlines()

    for q in search_terms:
        res = build_que(api, q)
        populate_unprocessed_que(res)