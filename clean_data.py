import pandas as pd
from cleantext import clean as cln


def clean(df):
    df['cleaned_text'] = df.text.apply(lambda x: cln(x, no_urls=True))
    return df
