import redis
import pandas as pd
import numpy as np
from gensim import corpora, models, similarities
from collections import defaultdict

REDIS_DB = 1
REDIS_PORT = 6379
REDIS_HOST = 'localhost'
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
pd.set_option('display.line_width', 300)

def _strip_element(x, ix):
    return x.split(':')[ix]


def _fetch_redis_element(key, hash, to_int=False):
    try:
        if to_int:
            return int(r.hget(key, hash))
        return r.hget(key, hash)
    except:
        return np.nan


def _strip_tags(tw):
    return ' '.join([x.replace('!', '').replace('.', '').lower().split(',')[0].split("'")[0] for x in tw.split(' ')
                     if ('#' not in x) and ('@' not in x) and (':' not in x) and (len(x) > 1) and (x != 'RT')])


def build_df(ix):
    keys = r.keys(ix)
    df = pd.DataFrame(keys, columns=['keys'])
    df['id'] = df['keys'].apply(_strip_element, args=[1])
    df['uid'] = df['keys'].apply(_strip_element, args=[2])
    df['dt'] = df['keys'].apply(_strip_element, args=[3])
    df['n_followers'] = df['keys'].apply(_fetch_redis_element, args=('user_follower_count', True))
    df['n_favorite'] = df['keys'].apply(_fetch_redis_element, args=('fav_count', True))
    df['n_retweet'] = df['keys'].apply(_fetch_redis_element, args=('rtw_count', True))
    df['query'] = df['keys'].apply(_fetch_redis_element, args=('q', False))
    df['tweet'] = df['keys'].apply(_fetch_redis_element, args=('tweet', False))
    df['words'] = df['tweet'].apply(_strip_tags)
    return df


df = build_df('upt:*')
df.drop_duplicates(cols='id', take_last=True, inplace=True)

#Get stoplist
with open('./db/word_count.csv') as f:
    stoplist = f.read().splitlines()
stoplist = [x.split(',')[0] for x in stoplist[1:int(float(len(stoplist)) * 0.045)]]

tweets = df['words'].tolist()
#Get additional stopwords
stopwords2 = ['tweet', 'too', 'me', 'ask', 'your', 'need', 'hint', 'by', 'for', 'under', 'about',
              'your', 'news', 'news.', 'thx', 'via', 'why', 'up', 'us', 'then',
              'make', '&amp;']
stoplist2 = stoplist + stopwords2

texts = [[word for word in document.lower().split() if word not in stoplist2] for document in tweets]
all_tokens = sum(texts, [])
tokens_once = set(word for word in set(all_tokens) if all_tokens.count(word) == 1)
texts = [[word for word in text if word not in tokens_once] for text in texts]

dictionary = corpora.Dictionary(texts)
dictionary.save('./db/news_corpus.dict')


corpus = [dictionary.doc2bow(text) for text in texts]
corpora.MmCorpus.serialize('./db/news_corpus.mm', corpus)

dictionary = corpora.Dictionary.load('./db/news_corpus.dict')
corpus = corpora.MmCorpus('./db/news_corpus.mm')

tfidf = models.TfidfModel(corpus)
corpus_tfidf = tfidf[corpus]
lsi = models.LsiModel(corpus_tfidf, id2word=dictionary, num_topics=15)

lsi.print_topics(2)

lsi.print_topic(2)


def sigmoid(x):
    return 1 / (1 + np.exp(-x))


def _score(tweet, lsi, dictionary):
    ncorp = dictionary.doc2bow(tweet)
    nscore = lsi[ncorp]
    highest_prob = sorted(nscore, key=lambda x: x[1])[-1]
    res = {'topic_id': highest_prob[0],
           'topic': None,
           'prob': highest_prob[1]}
    return res

#Score Un-processed Tweets
d = []
keys = r.keys('upt:*')
for key in keys:
    tweet = r.hget(key, 'tweet')
    tweet = ' '.join([word for word in _strip_tags(tweet).split(' ') if word not in stoplist2])
    try:
        score = _score(tweet, lsi, dictionary)
        new = [key, tweet, score['topic_id'], score['topic'], score['prob']]
        d.append(new)
        print tweet + ': ' + str(_score(tweet, lsi, dictionary)['prob'])
    except IndexError:
        pass

dfs = pd.DataFrame(d, columns=['key', 'tweet', 'topic_id', 'topic', 'prob'])
dfs['id'] = dfs['key'].apply(_strip_element, args=[1])
dfs.drop_duplicates(cols='id', take_last=True, inplace=True)
dfs.drop_duplicates(cols='tweet', take_last=True, inplace=True)
dfs.sort('prob', inplace=True)

quality_tweets = dfs['key'].tolist()[int(len(dfs) * 0.75):]

def _get_quality_tweets(dfs):
    return dfs['key'].tolist()[int(len(dfs) * 0.75):]


def _populate_unclassified_tweets(quality_tweets, batch_id):
    """
    Schema: batch-id:unc:twitter-handle:tweet-id
    """
    for key in quality_tweets:
        unprocessed_tweet = r.hgetall(key)
        twitter_handle = unprocessed_tweet['screen_name']
        tweet_id = key.split(':')[1]
        r.hmset('%s:unc:%s:%s' % (batch_id, twitter_handle, tweet_id), unprocessed_tweet)
        print "Posted: %s" % unprocessed_tweet['tweet']

dfs_quality = _get_quality_tweets(dfs)
