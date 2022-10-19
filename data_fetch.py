import pandas as pd
import tweepy as tw
from datetime import datetime, timedelta

import config

# populate the dataframe
def tdf(tweets, tweets_df, search):
    for tweet in tweets:
        tweets_df = pd.concat(
            [tweets_df, pd.DataFrame({
                'user_name': [tweet.user.name], 
                'user_location': [tweet.user.location],
                'user_description': [tweet.user.description],
                'user_verified': [tweet.user.verified],
                'followers_cnt': [tweet.user.followers_count],
                'account_date': [tweet.user.created_at.strftime("%Y-%m-%d")],
                'result_type': [tweet.metadata['result_type']],
                'favourites_cnt': [tweet.user.favourites_count],
                'date': [tweet.created_at.strftime("%Y-%m-%d")],
                'time': [tweet.created_at.strftime("%H:%M:%S")],
                'text': [tweet.full_text], 
                'hashtags': str([i['text'] for i in tweet.entities["hashtags"] if i]),
                'source': [tweet.source],
                'favourite_cnt': [tweet.favorite_count],
                'retweet_cnt': [tweet.retweet_count],
                'reply_uid': [tweet.in_reply_to_user_id],
                'reply_sm': [tweet.in_reply_to_status_id],
                'search_term': [search]
            })], ignore_index=True)

    return tweets_df

def fetch(searches=[]):
    tweets_df = pd.DataFrame()
    # authenticate
    auth = tw.OAuthHandler(config.my_api_key1, config.my_api_secret1)
    api = tw.API(auth, wait_on_rate_limit=True)
    for search in searches:
        search_query = search +" -filter:retweets"
   
        #tweets = tw.Cursor(api.search_tweets, 
        #                   q=search_query,
        #                   lang="en",
        #                  tweet_mode='extended'
        #                  ).items(4000)

        #tweets_df = tdf(tweets, tweets_df, search)
        
        tweets = api.search_tweets(q=search_query,
                                    lang="en", 
                                    count = 100,
                                    result_type="popular",
                                    tweet_mode='extended')

        tweets_df = tdf(tweets, tweets_df, search)

        tweets = api.search_tweets( q=search_query,
                                    lang="en", 
                                    until=(datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d"),
                                    count = 100,
                                    result_type="recent",
                                    tweet_mode='extended')

        tweets_df = tdf(tweets, tweets_df, search)

        id1 = max([i.id for i in tweets])

        for d in range(6,0,-1):
            tweets = api.search_tweets(
                                    q=search_query,
                                    lang="en",
                                    until=(datetime.today() - timedelta(days=d)).strftime("%Y-%m-%d"),
                                    count = 100,
                                    tweet_mode='extended',
                                    result_type="recent",
                                    since_id = id1
                                    )
            if len(tweets)>0:
                id1 = max([i.id for i in tweets])

                tweets_df = tdf(tweets, tweets_df, search)
                
        tweets_df.drop_duplicates(inplace=True, ignore_index=True)
        
        #print(f"\nFor search term : {search}")
        #print(min(tweets_df.date))
        #print(max(tweets_df.date))
        #print(f"Latest length of dataset: {len(tweets_df)}")
        
    l = tweets_df.columns.to_list()
    l.remove('search_term')
    
    print("Tweets before deletion\t:",tweets_df.shape)
    tweets_df.drop_duplicates(subset=l, ignore_index=True, inplace=True)
    print("Tweets after deletion\t:",tweets_df.shape)

    #tweets_df.to_excel(f'{config.data_dir}{"_".join(searches)}_{datetime.now().strftime("%d-%m-%y")}.xlsx', index=False)
    #tweets_df.to_excel(f'{config.data_dir}{"_".join(searches)}.xlsx', index=False)
    
    return tweets_df
