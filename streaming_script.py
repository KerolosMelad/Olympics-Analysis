import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
import socket
import json
from textblob import TextBlob, sentiments
from datetime import datetime , date
from os import path,listdir
import pandas as pd
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator



def Get_tweets(task_instance):
    consumer_key='uDlVzQSgq99mRln8MjBPVYeNR'
    consumer_secret='2rZslo6UcO89CXtBJtwxhw4l4cgBVgmjiz9TFUDx4vUTXKYEEp'
    access_token ='1438092111409291266-FtQ6ayZ5VR7Zsdfyh0py3VPRtzUSmf'
    access_secret='qM2m1sJzBa34TfRHszXPyZeRrSbwBktw7F6fZeAVWFaUH'

    class MyListener(tweepy.Stream):
        tweet_count = 0
        tweets =[] 
        def on_data(self, data):
            if(self.tweet_count>19): 
                self.tweet_count = 0
                self.disconnect()

            try:
                with open('python.json', 'a') as f:
                    print(self.tweet_count)
                    tweet = json.loads(data)['text']
                    self.tweets.append(tweet)
                    print(tweet)
                    self.tweet_count+=1
                    return True
            except BaseException as e:
                print("Error on_data: %s" % str(e))
            return True

        def on_error(self, status):
            print(status)
            return True

    twitter_stream = MyListener(consumer_key, consumer_secret,access_token, access_secret)
    twitter_stream.filter(track=['USA'])
    tweets_usa = twitter_stream.tweets 
    
    twitter_stream.filter(track=['Egypt'])
    tweets_eg= twitter_stream.tweets 

    task_instance.xcom_push(key='tweets', value=[tweets_usa, tweets_eg])
    


def sentiment_analysis(task_instance):

    tweets = task_instance.xcom_pull(key='tweets', task_ids='Get_tweets')

    tweets_usa = tweets[0]
    tweets_eg = tweets[1]

    sentiments_usa =[] 
    sentiments_eg=[] 
    
    for tweet in tweets_usa :
        sentiment = TextBlob(tweet).polarity  #The polarity score is a float within the range [-1.0, 1.0]
        sentiments_usa.append(sentiment)
        
    for tweet in tweets_eg:
        sentiment = TextBlob(tweet).polarity  #The polarity score is a float within the range [-1.0, 1.0]
        sentiments_eg.append(sentiment)
        
    task_instance.xcom_push(key='analysis', value=[sentiments_usa,sentiments_eg])

def avg_sentiment_analysis(task_instance):
    
    sentiments = task_instance.xcom_pull(key='analysis', task_ids='sentiment_analysis')

    sentiments_usa = sentiments[0]
    sentiments_eg = sentiments[1]

    if path.exists('Log.csv'): 
        lOG = pd.read_csv('Log.csv')
    else:
        lOG = pd.DataFrame({'Country' : [],'AVG_sentiment':[],'time':[]})


    avg_usa = sum(sentiments_usa)/20
    avg_eg = sum(sentiments_eg)/20  
    now = datetime.now()
    dt_string = now.strftime("%D:%H:%M")
    

    lOG=lOG.append({'Country' :'USA' ,'AVG_sentiment': avg_usa,'time':dt_string } ,ignore_index=True)
    
    lOG= lOG.append({'Country' :'Egypt' ,'AVG_sentiment': avg_eg,'time':dt_string } ,ignore_index=True)
    lOG.to_csv('Log.csv', index = False)
    task_instance.xcom_push(key='average', value=[avg_usa,avg_eg])

def compare_results(task_instance):
    
    avgs = task_instance.xcom_pull(key='average', task_ids='avg_sentiment_analysis')

    avg_usa = avgs[0]
    avg_eg = avgs[1]

    print("USA got the first rank")
    print('Avg. USA polarity' ,avg_usa)
    if avg_usa > 0 :
        print(" the setiment is positive")
    elif avg_usa < 0 :
        print(" the setiment is negative")
    else :
        print(" the setiment is neutral")
        
    print("\n")
    print("Egypt got the 54th rank")
    print('Avg. Egypt polarity' ,avg_eg)
    if avg_eg > 0 :

        print(" the setiment is positive")
    elif avg_eg < 0 :
        print(" the setiment is negative")
    else :
        print(" the setiment is neutral")
    


default_args= {
    'owner':'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 12, 27),
    'catchup': True
}

with DAG(
    'tweets-dag',
    description='tweets dag',
    schedule_interval="@daily",
    default_args=default_args
) as dag:

   get = PythonOperator(
       task_id="Get_tweets",
       python_callable=Get_tweets,
       provide_context=True,
   )

   
   analysis = PythonOperator(
       task_id="sentiment_analysis",
       python_callable=sentiment_analysis,
       provide_context=True,
   )

   
   avg = PythonOperator(
       task_id="avg_sentiment_analysis",
       python_callable=avg_sentiment_analysis,
       provide_context=True,
   )

   compare = PythonOperator(
       task_id="compare_results",
       python_callable=compare_results,
       provide_context=True,
   )

   get >> analysis >> avg >> compare