from etls.reddit_etl import connect_reddit,extract_posts,transform_data,load_data_to_csv
from utils.constants import CLIENT_ID,SECRET,OUTPUT_PATH
import pandas as pd
import numpy as np


def reddit_pipeline (file_name:str, subreddit:str, time_filter='day',limit=None):
    #Connecting to reddit instance
    isinstance = connect_reddit(CLIENT_ID,SECRET,'PathStreet4472')
    #Extraction
    posts = extract_posts(isinstance, subreddit, time_filter, limit)
    post_df = pd.DataFrame(posts)
    #Transformation
    post_df = transform_data(post_df)
    #Loading TO CSV
    file_path = f'{OUTPUT_PATH}/{file_name}.csv'
    load_data_to_csv(post_df, file_path)

    return file_path

