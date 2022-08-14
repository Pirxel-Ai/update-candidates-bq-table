import pandas as pd
from tqdm import tqdm_notebook as tqdm
import tweepy
import logging as log
import time
from src.utils import read_config
from src.scam_analyzer.scam import ScamDetector, search_words
from src.fetch_data import (create_dataset, list_datasets, get_dataset, update_dataset, 
                            delete_dataset, create_table, insert_rows, delete_table, 
                            list_tables, insert_df, query_datasets)
from flask import Flask
import os


config = read_config('../settings/config.ini')

app = Flask(__name__)

# Authenticate to Twitter
bearer_token = config['PirxBot']['BEARER_TOKEN']
consumer_key = config['PirxBot']['API_KEY']
consumer_secret_key = config['PirxBot']['API_SECRET']
access_token = config['PirxBot']['ACCES_TOKEN']
access_token_secret = config['PirxBot']['ACCES_TOKEN_SECRET']

client = tweepy.Client(bearer_token=bearer_token,
                      consumer_key=consumer_key,
                      consumer_secret=consumer_secret_key,
                      access_token=access_token,
                      access_token_secret=access_token_secret,
                      )


detector = ScamDetector(source='Twitter')

TABLE_ID = 'deeplogo.Fraud.candidates'

list_cols = [
 'target',
 'match_type',
 'id',
 'id_str',
 'name',
 'screen_name',
 'location',
 'description',
 'url',
 'protected',
 'followers_count',
 'friends_count',
 'listed_count',
 'created_at',
 'favourites_count',
 'verified',
 'statuses_count',
 'lang',
 'profile_image_url',
]

def get_detected_candidates(table_id):

    QUERY = (
            f"SELECT * FROM `{table_id}`"
        )

    rows = list(query_datasets(QUERY))

    df_candidates = pd.DataFrame([dict(x) for x in rows])
    list_ids = df_candidates.id.unique().tolist()

    analyzed_screen_name_list = df_candidates['screen_name'].unique().tolist()
    
    return analyzed_screen_name_list, list_ids

def get_followings(client):

    self_data = client.get_me()
    following_list = []
    next_token = None
    while True:
        partial_list = client.get_users_following(self_data[0].id, pagination_token=next_token, user_auth=True)
        for user in tqdm(partial_list[0]):
            following_list.append(user) 
        try:
            next_token = partial_list[3].get('next_token')
            if not next_token:
                break
        except tweepy.RateLimitError:
            time.sleep(60 * 15)
            continue
        except StopIteration:
            break
        
    scree_name_list = [x.username for x in following_list]
    
    return scree_name_list

scree_name_list = get_followings(client)
    
@app.route("/")
def stream_and_insert():
    analyzed_screen_name_list, list_ids = get_detected_candidates(TABLE_ID) 
    detector.list_reported =  analyzed_screen_name_list
    results = []
    for screen_name in scree_name_list:
        print(f'Target: {screen_name}')
        try:
            user_info = detector.tw.get_user_info(screen_name, user_id=False)
            if user_info is not None:
                predictions = detector.find_similar_users(user_info)
                print(f'Found {len(predictions)} candidates')
                for p in predictions:
                    d = {}
                    d['target'] = screen_name
                    d['match_type'] = p['match_type']
                    d['user_info'] = p['user_info']
                    results.append(d)
        except:
            pass
    
    try:
        df = pd.DataFrame(results)
        df = df.join(df.user_info.apply(pd.Series))
        del df['user_info']
        
        # APPEND
        if len(df)>0:
            df_to_append = df[~df.id.isin(list_ids)]
            df_to_append = df_to_append[list_cols]
            df_to_append.reset_index(drop=True, inplace=True)
            insert_df(df_to_append, TABLE_ID)
    except Exception as e:
        log.info(e)
        return 'Error'

    return 'OK'

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))