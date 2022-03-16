#import findspark
#findspark.init()

import json, requests, sys
from nltk.corpus import stopwords
from operator import add
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from textblob import TextBlob

# text classification
def getSentiment(text):
    sent = TextBlob(text).sentiment.polarity
    neutral_threshold = 0.05
    
    if sent >= neutral_threshold:
        return (1, 0, 0) # positive
    elif sent > -neutral_threshold:
        return (0, 1, 0) # neutral
    else:
        return (0, 0, 1) # negative
        
def getTweetsCounter(dstream_tweets_sentiment_analysed, window_length, sliding_interval):

    tweets_to_count = dstream_tweets_sentiment_analysed. \
        map(lambda x: ('count', (1, x[2])))

    tweets_count_acc_sent = tweets_to_count. \
        reduceByKeyAndWindow(lambda x, y: (x[0] + y[0], (x[1][0] + y[1][0], x[1][1] + y[1][1], x[1][2] + y[1][2])), None,
                             window_length, sliding_interval)

    total_count = tweets_count_acc_sent
    
    total_count.pprint()
    return total_count
    
def sendTweetsCounter(sentiments, url):
    def takeAndSend(time, rdd):
        if not rdd.isEmpty():
            (name, (total, (pos, neutral, neg))) = rdd.first()

            json_data = {'positive': pos, 'neutral': neutral, 'negative': neg, 'total': total}
            #print(json_data)

            response = requests.post(url, data=json_data)

    sentiments.foreachRDD(takeAndSend)
    
 def getTweets(kvs, sliding_interval):
    tweets_text = kvs.map(lambda x: json.loads(x)) \
                .map(lambda json_object: (json_object["user"]["screen_name"], json_object["text"], json_object["user"]["followers_count"], json_object["id"])) \
                .window(sliding_interval,sliding_interval) \
                .transform(lambda rdd: rdd.sortBy(lambda x: x[2], ascending = False))
    
    tweets_text.pprint()
    return tweets_text
    
def sendTweets(tweets, url):
    def takeAndSend(time, rdd):
        if not rdd.isEmpty():
            tweets_data = rdd.take(10)

            users = []
            texts = []
            tweet_ids = []

            for (user, text, follower_count, tweet_id) in tweets_data:
                users.append(user)
                texts.append(text)
                tweet_ids.append(tweet_id)

            json_data = {'user': str(users), 'text': str(texts), 'id': str(tweet_ids)}
            #print(json_data)

            response = requests.post(url, data=json_data)

    tweets.foreachRDD(takeAndSend)
    
def getTopWords(tweets, window_length, sliding_interval):
    words = tweets.map(lambda line:re.sub(r'http\S+','',line[1])) \
                  .map(lambda line:re.sub(r'bit.ly/\S+','', line)) \
                  .map(lambda line:line.strip('[link]')) \
                  .flatMap(lambda line: re.split(r"[\n;,\.\s]",line))

    ## This part does the word count
    sw = stopwords.words('english')
    sw.extend(['rt']+keyword)
    
    counts = words.map(lambda word: word.strip().lower()) \
                  .filter(lambda word: word not in sw) \
                  .filter(lambda word: len(word) >= 2 and word[0] != '#' and word[0] != '@') \
                  .map(lambda word: (word, 1)) \
                  .reduceByKeyAndWindow(add, None,  window_length, sliding_interval)\
                  .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending = False))
    
    counts.pprint()
    return counts
    
 def getTopHashTags(tweets, window_length, sliding_interval):
    words = tweets.map(lambda line:re.sub(r'http\S+','',line[1])) \
                  .map(lambda line:re.sub(r'bit.ly/\S+','', line)) \
                  .map(lambda line:line.strip('[link]')) \
                  .flatMap(lambda line: re.split(r"[\n;,\.\s]",line))

    hashtags = words.map(lambda word: word.strip().lower()) \
            .filter(lambda word: len(word) >= 2 and word[0] == '#') \
            .map(lambda word: (word, 1)) \
            .reduceByKeyAndWindow(add, None,  window_length, sliding_interval)\
            .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending = False))
    
    hashtags.pprint()
    return hashtags
    
def sendTopWords(counts, url, num):
    def takeAndSend(time, rdd):
        if not rdd.isEmpty():
            word_counts = rdd.take(num)

            words = []
            values = []

            for (word, count) in word_counts:
                words.append(word)
                values.append(count)

            json_data = {'words': str(words), 'counts': str(values)}
            print(json_data)

            response = requests.post(url, data=json_data)

    counts.foreachRDD(takeAndSend)
    
sc = SparkContext(appName="tweetStream")
# Create a local StreamingContext with batch interval of 2 second
batch_interval = 2
window_length = 15*60
sliding_interval = 6

ssc = StreamingContext(sc, batch_interval)
ssc.checkpoint("twittercheckpt")

# Create a DStream that conencts to hostname:port
tweetStream = ssc.socketTextStream("0.0.0.0", 5555)
    
    
tweets = tweetStream. \
        map(lambda  x: json.loads(x)). \
        map(lambda json_object: (json_object["user"]["screen_name"], json_object["text"]))

tweets_sentiment_analysed = tweets. \
        map(lambda x: (x[0], x[1], getSentiment(x[1])))
        
server = 'http://localhost:5000/'

tweet_counters = getTweetsCounter(tweets_sentiment_analysed, window_length, sliding_interval)
sendTweetsCounter(tweet_counters,  server +'update_sentiments')

tweet_text= getTweets(tweetStream, sliding_interval)
sendTweets(tweet_text, server + 'update_tweets')

key_words=getTopWords(tweets, window_length, sliding_interval)
sendTopWords(key_words, server + 'update_counts', 10)

hashtag=getTopHashTags(tweets, window_length, sliding_interval)
sendTopWords(hashtag, server + 'update_hashtagcounts', 30)
    
# Start computing
ssc.start()        
# Wait for termination
ssc.awaitTermination()
ssc.stop(stopGraceFully = True)