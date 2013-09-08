import redis
import pandas as pd
import numpy as np
from gensim import corpora, models, similarities
from collections import defaultdict
from datetime import datetime, timedelta, date
import pickle
import csv
import ConfigParser
import argparse

REDIS_DB = 1
REDIS_PORT = 6379
REDIS_HOST = 'localhost'
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
pd.set_option('display.line_width', 300)


class GetTopics:
    def __init__(self, batch_id):
        self.batch_id = batch_id


    @staticmethod
    def _strip_element(x, ix):
        return x.split(':')[ix]

    @staticmethod
    def _fetch_redis_element(key, hash, to_int=False):
        try:
            if to_int:
                return int(r.hget(key, hash))
            return r.hget(key, hash)
        except:
            return np.nan

    @staticmethod
    def _strip_tags(tw):
        return ' '.join([x.replace('!', '').replace('.', '').lower().split(',')[0].split("'")[0] for x in tw.split(' ')
                         if ('#' not in x) and ('@' not in x) and (':' not in x) and (len(x) > 1) and (x != 'RT')])

    @staticmethod
    def sigmoid(x):
        return 1 / (1 + np.exp(-x))

    def build_df(self, ix):
        keys = r.keys(ix)
        df = pd.DataFrame(keys, columns=['keys'])
        df['id'] = df['keys'].apply(self._strip_element, args=[1])
        df['uid'] = df['keys'].apply(self._strip_element, args=[2])
        df['dt'] = df['keys'].apply(self._strip_element, args=[3])
        df['n_followers'] = df['keys'].apply(self._fetch_redis_element, args=('user_follower_count', True))
        df['n_favorite'] = df['keys'].apply(self._fetch_redis_element, args=('fav_count', True))
        df['n_retweet'] = df['keys'].apply(self._fetch_redis_element, args=('rtw_count', True))
        df['query'] = df['keys'].apply(self._fetch_redis_element, args=('q', False))
        df['tweet'] = df['keys'].apply(self._fetch_redis_element, args=('tweet', False))
        df['words'] = df['tweet'].apply(self._strip_tags)
        return df

    @staticmethod
    def _score_all(tweet, lsi, dictionary):
        ncorp = dictionary.doc2bow(tweet.lower().split())
        nscore = lsi[ncorp]
        return nscore

    def _score(self, tweet, lsi, dictionary):
        ncorp = dictionary.doc2bow(tweet.lower().split())
        nscore = lsi[ncorp]
        highest_prob = sorted(nscore, key=lambda x: x[1])[-1]
        res = {'topic_id': highest_prob[0],
               'topic': None,
               'prob': highest_prob[1]}
        return res

    def _score_upt(self, keys, lsi, dictionary, stoplist):
        #Score Un-processed Tweets
        d = []
        for key in keys:
            tweet = r.hget(key, 'tweet')
            tweet = ' '.join([word for word in self._strip_tags(tweet).split(' ') if word not in stoplist])
            try:
                score = self._score(tweet, lsi, dictionary)
                new = [key, tweet, score['topic_id'], score['topic'], score['prob']]
                d.append(new)
            except IndexError:
                pass
        dfs = pd.DataFrame(d, columns=['key', 'tweet', 'topic_id', 'topic', 'prob'])
        dfs['id'] = dfs['key'].apply(self._strip_element, args=[1])
        dfs.drop_duplicates(cols='id', take_last=True, inplace=True)
        dfs.drop_duplicates(cols='tweet', take_last=True, inplace=True)
        dfs.sort('prob', inplace=True)
        return dfs

    def _get_quality_tweets(self, dfs):
        d = {'junk': dfs['key'].tolist()[:int(len(dfs) * 0.10)],
             'keep_around': dfs['key'].tolist()[int(len(dfs) * 0.10) + 1:int(len(dfs) * 0.75)],
             'quality': dfs['key'].tolist()[int(len(dfs) * 0.75) + 1:]}
        return d

    def _populate_unclassified_tweets(self, quality_tweets, batch_id):
        """
        Schema: batch-id:unc:twitter-handle:tweet-id
        """
        for key in quality_tweets:
            unprocessed_tweet = r.hgetall(key)
            twitter_handle = unprocessed_tweet['screen_name']
            tweet_id = key.split(':')[1]
            r.hmset('%s:unc:%s:%s' % (batch_id, twitter_handle, tweet_id), unprocessed_tweet)
            #print "Posted: %s" % unprocessed_tweet['tweet']

    def _write_tweet_to_csv(self, keys, outfile, header):
        o = []
        for key in keys:
            attr = r.hgetall(key)
            values = attr.keys()
            if values == header:
                o.append([key] + attr.values())
        with open(outfile, 'wb') as f:
            writer = csv.writer(f)
            writer.writerows(o)
        return o

    def _delete_keys(self, keys):
        for key in keys:
            r.delete(key)

    def _old_upt_keys(self, keys, ndays):
        d = date.today() - timedelta(days=ndays)
        o = []
        for key in keys:
            key_dt = datetime.strptime(key.split(':')[3][:8], '%Y%m%d').date()
            if key_dt < d:
                o.append(key)
        return o

    @staticmethod
    def _strip_date(x):
        return datetime.strptime(x.split(':')[3][:8], '%Y%m%d').date()

    @staticmethod
    def _old_tweet(x, ndays):
        if x < (date.today() - timedelta(days=ndays)):
            return 1
        return 0



if __name__ == '__main__':
    #set inputs from commandline
    parser = argparse.ArgumentParser(description='Read tweet processing config file.')
    parser.add_argument('-c', dest='conf', required=True, help='Config File')

    args = parser.parse_args()
    twcf = args.conf

    #Process Configurator File
    config = ConfigParser.ConfigParser()
    config.read(twcf)

    path = config.get('BACKUP', 'path')
    print "Loaded Configuration"

    cur_batch_id = max([int(x) for x in r.smembers('batch-id')])
    batch_id = cur_batch_id + 1
    dt = str(datetime.now()).replace(':', '').replace('.', ''). replace('-', '').replace(' ', '')
    print "Current batch id is %s. New batch id is %s." % (cur_batch_id, batch_id)

    handle = GetTopics(batch_id)

    df = handle.build_df('upt:*')
    df['timestamp'] = df['keys'].apply(handle._strip_date)
    df['old_tweet'] = df['timestamp'].apply(handle._old_tweet, args=[1])
    df_old_tweets = df[df['old_tweet'] == 1]
    df = df[df['old_tweet'] == 0]
    df_cur = handle.build_df('%s:unc:*' % cur_batch_id)
    df = pd.concat([df, df_cur])
    df.drop_duplicates(cols='id', take_last=True, inplace=True)
    print "Built a dataframe with tweets to build topics from. # of records: %s" % len(df)
    #Build a dictionary from unprocessed tweets

    #Get stoplist
    with open('%s/word_count.csv' % path) as f:
        stoplist = f.read().splitlines()
    stoplist = [x.split(',')[0] for x in stoplist[1:int(float(len(stoplist)) * 0.045)]]

    tweets = df['words'].tolist()
    #Get additional stopwords
    stopwords2 = ['tweet', 'too', 'me', 'ask', 'your', 'need', 'hint', 'by', 'for', 'under', 'about',
                  'your', 'news', 'news.', 'thx', 'via', 'why', 'up', 'us', 'then',
                  'make', '&amp;', 'says', '"the', 'lets']
    stoplist2 = stoplist + stopwords2

    #build dictionary and corpus
    texts = [[word for word in document.lower().split() if word not in stoplist2] for document in tweets]
    all_tokens = sum(texts, [])
    tokens_once = set(word for word in set(all_tokens) if all_tokens.count(word) == 1)
    texts = [[word for word in text if word not in tokens_once] for text in texts]

    dictionary = corpora.Dictionary(texts)
    dictionary.save('%s/news_corpus_%s.dict' % (path, dt))
    print "Built and saved dictionary to: %s/news_corpus_%s.dict" % (path, dt)

    corpus = [dictionary.doc2bow(text) for text in texts]
    corpora.MmCorpus.serialize('%s/news_corpus_%s.mm' % (path, dt), corpus)
    print "Built and saved corpus to: %s/news_corpus_%s.mm" % (path, dt)

    dictionary = corpora.Dictionary.load('%s/news_corpus_%s.dict' % (path, dt))
    corpus = corpora.MmCorpus('%s/news_corpus_%s.mm' % (path, dt))

    #Build topical models
    tfidf = models.TfidfModel(corpus)
    corpus_tfidf = tfidf[corpus]
    lsi = models.LsiModel(corpus_tfidf, id2word=dictionary, num_topics=15)
    print "Built topicalm model"
    for i in range(0, 15):
        print lsi.print_topic(i)

    #Pickle model
    output = open('%s/lsi_model_%s.pkl' % (path, dt), 'wb')
    pickle.dump(lsi, output)
    output.close()
    print "pickled lsi: %s/lsi_model_%s.pkl" % (path, dt)

    #Score unprocessed tweets
    header = ['rtw_count', 'screen_name', 'tweet', 'q', 'user_follower_count', 'user_image_url', 'created_at', 'fav_count', 'name']
    keys = r.keys('upt:*')
    dfs_quality = handle._score_upt(keys, lsi, dictionary, stoplist2)
    print "Got %s quality tweets!" % len(dfs_quality)

    scored_tweets = handle._get_quality_tweets(dfs_quality)
    quality_tweets = scored_tweets['quality']
    handle._populate_unclassified_tweets(quality_tweets, batch_id)
    print "populated redis db with quality tweets. batch_id: %s." % batch_id
    #Backup quality tweets
    outf = handle._write_tweet_to_csv(quality_tweets, '%s/bkup_quality_%s.csv' % (path, dt), header)
    handle._delete_keys(quality_tweets)
    print "Backed up quality tweets to %s/bkup_quality_%s.csv." % (path, dt)

    #Backup junk tweets
    outf = handle._write_tweet_to_csv(scored_tweets['junk'], '%s/bkup_junk_%s.csv' % (path, dt), header)
    handle._delete_keys(scored_tweets['junk'])
    print "Backed up junk tweets to%s/bkup_junk_%s.csv." % (path, dt)

    #Backup keep-around tweets
    outf = handle._write_tweet_to_csv(scored_tweets['keep_around'], '%s/bkup_keep_around_%s.csv' % (path, dt), header)
    print "Backed up keep-around tweets to %s/bkup_keep_around_%s.csv" % (path, dt)

    #Backup and delete old-unprocessed tweets (more than 2 days old)
    old_keys = handle._old_upt_keys(r.keys('upt:*'), 1)
    handle._delete_keys(old_keys)
    r.sadd('batch-id', batch_id)
    print "Done!!!!"
