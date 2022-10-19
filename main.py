import pandas as pd
from data_fetch import fetch
from clean_data import clean
from model import vader_run

print('###################### TweetItBig ######################')

# The keywords to scrape and analyze upon
searches = ["iphone", "iphone14", "iphone 14", "ios", "ios16", "ios 16", "apple"]
print("Scraping data for search terms: ", searches)

# Fetching data around the keywords
tweets_df = fetch(searches)

# Cleaning rules for the tweet's text
tweets_df = clean(tweets_df)

print("Shape of data extracted today: ", tweets_df.shape)

print("\n\nRunning Vader...")
# Extracting sentiment
tweets_df = vader_run(tweets_df)

print("Resultant sentiment counts: ")
print(tweets_df.Vader_sentiment.value_counts())
