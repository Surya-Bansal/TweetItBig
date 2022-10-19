import config
import pandas as pd
import nltk
nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer

def vader_run(df):
    df["Vader_score"] = [SentimentIntensityAnalyzer().polarity_scores(i)['compound'] for i in df["text"]]

    cutoff = 0
    df["Vader_sentiment"] = ''
    df.loc[df.Vader_score > cutoff, 'Vader_sentiment'] = 'Positive'
    df.loc[df.Vader_score.abs() <= cutoff, 'Vader_sentiment'] = 'Neutral'
    df.loc[df.Vader_score < -cutoff, 'Vader_sentiment'] = 'Negative'

    #df["Vader_sentiment"].value_counts()

    old = pd.read_excel(config.data_dir+'Final_output.xlsx')
    old.date = old.date.astype(str)
    
    full_df = pd.concat([old, df], ignore_index=True)

    l = full_df.columns.to_list()
    l.remove('search_term')
    print("Shape with duplicates\t: ",full_df.shape)
    full_df.drop_duplicates(subset=l, ignore_index=True, inplace=True)
    print("Shape without duplicates\t: ",full_df.shape)

    full_df.to_excel(config.data_dir+'Final_output.xlsx', index=False)

    return df
