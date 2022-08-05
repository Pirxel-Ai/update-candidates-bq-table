import pandas as pd
import sys
import logging
import os

workdir = os.path.dirname(os.path.abspath(__file__))
MAIN_LIBRARY_PATH = str(workdir) + '/main-library'
sys.path.append(str(MAIN_LIBRARY_PATH))
from src.scam_analyzer.scam import ScamDetector, search_words
from src.fetch_data import (create_dataset, list_datasets, get_dataset, update_dataset, 
                            delete_dataset, create_table, insert_rows, delete_table, 
                            list_tables, insert_df, query_datasets)
from flask import Flask

app = Flask(__name__)

detector = ScamDetector('Twitter')

TABLE_ID = 'deeplogo.Fraud.candidates'

screen_name_list = [
            'ICBCArgentina', 
            'BancoGalicia', 
            'bbva_argentina', 
            'Santander_Ar',
            'HSBC_AR',
            'banco_patagonia',
            'bancomacro',
            'MODO_Arg',
            'uala_arg',
            'mercadopago',
            'RappiArgentina',
            'PedidosYA'
                   ] 



QUERY = (
        "SELECT id FROM `deeplogo.Fraud.candidates`"
    )

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
# 'entities',
 'protected',
 'followers_count',
 'friends_count',
 'listed_count',
 'created_at',
 'favourites_count',
# 'utc_offset',
# 'time_zone',
# 'geo_enabled',
 'verified',
 'statuses_count',
 'lang',
# 'status',
# 'contributors_enabled',
# 'is_translator',
# 'is_translation_enabled',
#  'profile_background_color',
#  'profile_background_image_url',
#  'profile_background_image_url_https',
#  'profile_background_tile',
 'profile_image_url',
#  'profile_image_url_https',
#  'profile_banner_url',
#  'profile_link_color',
#  'profile_sidebar_border_color',
#  'profile_sidebar_fill_color',
#  'profile_text_color',
#  'profile_use_background_image',
#  'has_extended_profile',
#  'default_profile',
#  'default_profile_image',
#  'following',
#  'follow_request_sent',
#  'notifications',
#  'translator_type',
#  'withheld_in_countries'
]    
    
@app.route("/")
def stream_and_insert():    
    results = []
    for screen_name in screen_name_list:
        try:
            user_info = detector.tw.get_user_info(screen_name, user_id=False)
            predictions = detector.find_similar_users(user_info)
            for p in predictions:
                d = {}
                d['target'] = screen_name
                d['match_type'] = p['match_type']
                d['user_info'] = p['user_info']
                results.append(d)
        except Exception as e:
            log.info(e)
            return 'Error'
    
    try:
        df = pd.DataFrame(results)
        df = df.join(df.user_info.apply(pd.Series))
        del df['user_info']
        rows = query_datasets(QUERY)

        # Filter out ids already in DB
        list_ids = [row['id'] for row in rows]
        df = df[~df.id.isin(list_ids)]
        print(len(df))

        if len(df)>0:
            insert_df(df[list_cols], TABLE_ID)
    except Exception as e:
        log.linfo(e)
        return 'Error'

    return 'OK'
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))