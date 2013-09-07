from __future__ import with_statement
from contextlib import closing
import redis
from random import shuffle
from flask import Flask, request, session, g, redirect, url_for, \
    abort, render_template, jsonify
from datetime import datetime
#-------------------------------------------------------------------------#
# How to launch app locally:
#    1. Make sure you have a configured redis.conf file (dbfilename)
#    2. Fire up the Redis server with you redis.conf file:
#       >>> /Users/msemeniuk/redis-2.6.7/src/redis-server ./redis.conf
#
#-------------------------------------------------------------------------#

# configuration
SECRET_KEY = 'development key'
REDIS_DB = 1
REDIS_PORT = 6379
REDIS_HOST = 'localhost'

app = Flask(__name__)
app.config.from_object(__name__)
app.config.from_envvar('FLASKR_SETTINGS', silent=True)

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)


def _get_sample_tweet():
    """
    Gets a set of sample tweets for testing. Here is how to create a test tweet:
    >>>q = {'name': 'Mikhail Semeniuk', 'screen_name': 'MikhailSemeniuk', 'tweet': 'some text here', 'fav_count': 1,\
            'rtw_count': 10, 'created_at': '2013-04-14', 'user_image_url': 'http://google.com', \
            'user_follower_count': 322}
    >>>r.hmset("20130831161225393296:test:MikhailSemeniuk:0", q)
    """


@app.route('/_refresh_results')
def random_tweet():
    error = None
    cur_batch_id = max([int(x) for x in r.smembers('batch-id')])
    tweets = r.keys("%s:unc:*" % cur_batch_id)
    shuffle(tweets)
    res = r.hgetall(tweets[0])
    return jsonify(result=res, error=error)


@app.route('/')
def homepage():
    error = None
    cur_batch_id = max([int(x) for x in r.smembers('batch-id')])
    tweets = r.keys("%s:unc:*" % cur_batch_id)
    shuffle(tweets)
    res = r.hgetall(tweets[0])
    return render_template('layout.html', error=error, que=res)

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0', port=5000)