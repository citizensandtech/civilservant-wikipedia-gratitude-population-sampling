#!/usr/bin/env python
# coding: utf-8

import json
import requests

from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
from pymysql.err import InternalError, OperationalError
import sys, os
import pandas as pd
import numpy as np

import mwclient
import mwviews
import mwapi
import mwreverts
import mwreverts.api

from datetime import datetime as dt
from datetime import timedelta as td

import time
import functools

def wmftimestamp(bytestring):
    if bytestring:
        s = bytestring.decode('utf-8')
        return dt.strptime(s, '%Y%m%d%H%M%S')
    else:
        return bytestring
    
def decode_or_none(b):
    return b.decode('utf-8') if b else None

sim_treatment_dates = (dt(2018,3,6), dt(2018,3, 23))
sim_treatment_date = sim_treatment_dates[0]

sim_experiment_end_date = sim_treatment_date + td(days=90)
sim_observation_start_date = sim_treatment_date - td(days=90)


def timeit(func):
    @functools.wraps(func)
    def newfunc(*args, **kwargs):
        startTime = time.time()
        func(*args, **kwargs)
        elapsedTime = time.time() - startTime
        print('function [{}] finished in {} ms'.format(
            func.__name__, int(elapsedTime * 1000)))
    return newfunc


# In[2]:


constr = 'mysql+pymysql://{user}:{pwd}@{host}:{port}'.format(user=os.environ['MYSQL_USERNAME'],
                                                      pwd=os.environ['MYSQL_PASSWORD'],
                                                      host=os.environ['MYSQL_HOST'],
                                                    port=os.environ['MYSQL_PORT'],
                                                         charset='utf8',
                                                        use_unicode=True)

con = create_engine(constr, encoding='utf-8')


con.execute(f'use enwiki_p;')


# In[3]:


#POPULATIONS

# ar_is_autoreviewer (binary): does this account have autoreviewer status? If the account isn't from that language, the value should be NA
# de_is_autoreviewer (binary): does this account have flagged revisions permission on DE Wikipedia?  If the account isn't from that language, the value should be NA
# de_is_days_enough (binary): has this user been active for 60 days
# de_is_edits_enough (binary): has the user more than 300 edits
# pl_is_editor (binary): does this account have flagged revisions permission on PL Wikipedia?  If the account isn't from that language, the value should be NA
# fa_is_days_enough (binary): is 365 days or more registered
# fa_is_edits_enough (binary): is 500 edits or more 
DE_EDITS_ENOUGH = 300
DE_DAYS_ENOUGH = 60
FA_EDITS_ENOUGH = 500
FA_DAYS_ENOUGH = 365 

de_pop_sql = """select user_id, ug_group, user_name, user_editcount, user_registration from (
    select user_id, ug_group, user_name, user_editcount, coalesce(user_registration, 20010101000000) as user_registration
          from (select * from user_groups where ug_group = 'autoreview') ug
join user u on  ug.ug_user = u.user_id) coal
where user_editcount >= {DE_EDITS_ENOUGH} and user_registration <= {reg_start};
    """.format(reg_start=(sim_treatment_date-td(days=DE_DAYS_ENOUGH)).strftime('%Y%m%d%H%M%S'), 
               DE_EDITS_ENOUGH=DE_EDITS_ENOUGH)

fa_pop_sql = """select * from (
    select user_id, user_name, user_editcount, coalesce(user_registration, 20010101000000) as user_registration from user) u
                    where user_editcount >= {FA_EDITS_ENOUGH} and user_registration <= {reg_start};
    """.format(reg_start=(sim_treatment_date-td(days=FA_DAYS_ENOUGH)).strftime('%Y%m%d%H%M%S'),
              FA_EDITS_ENOUGH=FA_EDITS_ENOUGH)
                    

def user_group_members(user_group):
    return """select * from (
select user_id, user_name, ug_group, coalesce(user_registration, 20010101000000) as user_registration 
from (select * from user_groups where ug_group = '{user_group}') ug
  join user u on ug.ug_user = u.user_id) coalesced
  where user_registration <= {reg_start};""".format(reg_start=sim_treatment_date.strftime('%Y%m%d%H%M%S'),
                                                   user_group=user_group)

lang_sqlparams = {'de': {'pop_sql': de_pop_sql, 'true_cols_to_add': ['de_is_days_enough', 'de_is_edits_enough', 'de_is_autoreviewer']},
                  'ar': {'pop_sql': user_group_members('autoreview'), 'true_cols_to_add': ['ar_is_autoreview']},
                  'pl': {'pop_sql': user_group_members('editor'), 'true_cols_to_add': ['pl_is_editor']},
                  'fa': {'pop_sql': fa_pop_sql, 'true_cols_to_add': ['fa_is_days_enough', 'fa_is_edits_enough']}
                }

def create_thanker_pop(lang, pop_sql, true_cols_to_add):
    cache_key = f'cache/pops/{lang}'
    if os.path.exists(cache_key):
        return pd.read_pickle(cache_key)
    else:
        con.execute(f'use {lang}wiki_p;')
        df = pd.read_sql(pop_sql, con)
        decode_cols = ['ug_group', 'user_name',]
        timestamp_cols = ['user_registration']
        for decode_col in decode_cols:
            try:
                df[decode_col] = df[decode_col].apply(decode_or_none)
            except KeyError:
                df[decode_col] = float('nan')
        for timestamp_col in timestamp_cols:
            df[timestamp_col] = df[timestamp_col].apply(wmftimestamp)
        for true_col_to_add in true_cols_to_add:
            df[true_col_to_add] = True

        df['lang'] = lang
        df.to_pickle(cache_key)
        return df


# In[76]:


#BLOCKS (aka. Bans)
def get_bans(lang, start_date, end_date):
    cache_key = f'cache/bans/{lang}_{start_date}_{end_date}.pickle'
    if not os.path.exists(cache_key):
        start_stamp = start_date.strftime('%Y%m%d%H%M%S')
        end_stamp = end_date.strftime('%Y%m%d%H%M%S')
        con.execute(f'use {lang}wiki_p;')
        ban_sql = f"""select log_user as blocking_user_id, log_user_text as blocking_user_name, log_title as blocked_user_name 
        from logging where log_action='block' 
        and log_timestamp >= {start_stamp} and log_timestamp < {end_stamp};"""
#         print(ban_sql)
        ban_df = pd.read_sql(ban_sql, con)
        ban_df['blocking_user_name'] = ban_df['blocking_user_name'].apply(decode_or_none)
        ban_df['blocked_user_name'] = ban_df['blocked_user_name'].apply(decode_or_none)
        ban_df['lang'] = lang
        ban_df = ban_df[pd.notnull(ban_df['blocking_user_id'])]
        ban_df.to_pickle(cache_key)
    else:
        ban_df = pd.read_pickle(cache_key)
    
    return ban_df

def add_blocks(start_date, end_date, col_label, df):
    ban_dfs = []
    for lang in lang_sqlparams.keys():
    #     print(lang)
        t0 = time.time()
        ban_df = get_bans(lang, start_date, end_date)
        ban_dfs.append(ban_df)
#         print(f'{lang} took {time.time()-t0}.')

    bans = pd.concat(ban_dfs)


    bans = bans.rename(columns={'blocking_user_id':'user_id'})
    user_ban_counts = pd.DataFrame(bans.groupby(['lang','user_id']).size()).reset_index()
    user_ban_counts['user_id'] = user_ban_counts['user_id'].apply(int)

    df = pd.merge(df, user_ban_counts, on=['lang', 'user_id'], how='left').rename(columns={0:col_label})
    return df

#@timeit
def add_blocks_pre_treatment(df):
    return add_blocks(sim_observation_start_date, sim_treatment_date, "block_actions_90_pre_treatment", df)

#@timeit
def add_blocks_post_treatment(df):
    return add_blocks(sim_treatment_date, sim_experiment_end_date, "block_actions_90_post_treatment", df)


# In[77]:


#Just cache user histories
def get_user_edits(lang, user_id, start_date, end_date):
    cache_key = f'cache/edithistory/{lang}_{user_id}_{start_date}_{end_date}.pickle'
    if not os.path.exists(cache_key):
        start_stamp = start_date.strftime('%Y%m%d%H%M%S')
        end_stamp = end_date.strftime('%Y%m%d%H%M%S')
        con.execute(f'use {lang}wiki_p;')
        user_sql = f"""select rev_id, rev_timestamp, page_id, page_namespace from
                        (select * from revision_userindex 
                         where rev_user = {user_id} and
                          rev_timestamp >= {start_stamp} and rev_timestamp < {end_stamp})
                        user_revs
                        join page where rev_page = page_id;"""

        user_df = pd.read_sql(user_sql, con)
        user_df['rev_timestamp'] = user_df['rev_timestamp'].apply(wmftimestamp)
        user_df['lang'] = lang

        user_df.to_pickle(cache_key)
    else:
        user_df = pd.read_pickle(cache_key)
    
    return user_df

#@timeit
def cache_all_user_edits(df):
    count = 0
    for lang in lang_sqlparams.keys():
        for start_date, end_date in ((sim_observation_start_date, sim_treatment_date), 
                                     (sim_treatment_date, sim_experiment_end_date)):
            user_ids = df[df['lang']==lang]['user_id'].values
            for user_id in user_ids:
#                 if count % 1000 == 0:
#                     print(f'Count {count} lang {lang} userid {user_id}')
                user_df = get_user_edits(lang, user_id, start_date, end_date)
                count += 1



# REVERTS
def get_num_reverts(lang, user_id, user_df, start_date, end_date):
    cache_key = f'cache/reverts/{lang}_{user_id}_{start_date}_{end_date}.pickle'
    if os.path.exists(cache_key):
        return pd.read_pickle(cache_key)
    else:
        session = mwapi.Session(f"https://{lang}.wikipedia.org", user_agent="max.klein@civilservant.io gratitude power analysis generator")
        revertings = 0
        for rev_id in user_df['rev_id'].values:
            try:
                reverting, reverted, reverted_to = mwreverts.api.check(session, rev_id)
                if reverting:
                    revertings += 1
            except mwapi.session.APIError:
                continue
        col_name_suffix = 'pre' if start_date < sim_treatment_date else 'post'
        col_name = f'num_reverts_90_{col_name_suffix}_treatment'
        user_reverts_df = pd.DataFrame.from_dict({col_name:[revertings],'user_id':[user_id], 'lang':[lang]}, orient='columns')
        user_reverts_df.to_pickle(cache_key)
        return user_reverts_df

def create_reverts_df(df, start_date, end_date):
    count = 0
    reverts_dfs = []
    for lang in lang_sqlparams.keys():
            user_ids = df[df['lang']==lang]['user_id'].values
            for user_id in user_ids:
#                 if count % 1000 == 0:
#                     print(f'Count {count} lang {lang} userid {user_id}')
                user_df = get_user_edits(lang, user_id, start_date, end_date)
                t0 = time.time()
                user_revert_df = get_num_reverts(lang, user_id, user_df, start_date, end_date)
#                 print(f'user_id {user_id} took {time.time()-t0} to get reverts')
                reverts_dfs.append(user_revert_df)
                count += 1
    reverts_df = pd.concat(reverts_dfs)
    return reverts_df

def create_and_merge_revert_actions(df, start_date, end_date):
    reverts_df = create_reverts_df(df, start_date, end_date)
    df = pd.merge(df, reverts_df, how='left', on=['lang', 'user_id'])
    return df

#@timeit
def add_revert_actions_pre_treatment(df):
    return create_and_merge_revert_actions(df, start_date=sim_observation_start_date, end_date=sim_treatment_date)

#@timeit
def add_revert_actions_post_treatment(df):
    return create_and_merge_revert_actions(df, start_date=sim_treatment_date, end_date=sim_experiment_end_date)
    


#TALKPAGES
def get_talk_counts(lang, user_id, user_df, start_date, end_date, namespace_fn):
    talk_count = user_df['page_namespace'].apply(namespace_fn['fn']).sum()
    user_talk_df = pd.DataFrame.from_dict({namespace_fn['col']:[talk_count],
                                           'user_id':[user_id], 
                                           'lang':[lang]}, orient='columns')
    return user_talk_df

def create_talk_df(df, start_date, end_date, namespace_fn):
    count = 0
    talk_dfs = []
    for lang in lang_sqlparams.keys():
            user_ids = df[df['lang']==lang]['user_id'].values
            for user_id in user_ids:
#                 if count % 1000 == 0:
#                     print(f'Count {count} lang {lang} userid {user_id}')
                user_df = get_user_edits(lang, user_id, start_date, end_date)
                t0 = time.time()
                user_talk_df = get_talk_counts(lang, user_id, user_df, start_date, end_date, namespace_fn)
#                 print(f'user_id {user_id} took {time.time()-t0} to get reverts')
                talk_dfs.append(user_talk_df)
    talk_df = pd.concat(talk_dfs)
    return talk_df


def create_and_merge_talk(df, start_date, end_date, namespace_fn):
    talk_df = create_talk_df(df, start_date, end_date, namespace_fn)
    df = pd.merge(df, talk_df, how='left', on=['lang', 'user_id'])
    return df

def is_wp_page(namespace):
    return namespace in [4,5] # maybe in (4,5)

def is_talk_page(namespace):
    return namespace % 2 == 1

#@timeit
def add_support_talk_90_pre_treatment(df):
    return create_and_merge_talk(df, start_date=sim_observation_start_date, end_date=sim_treatment_date,
                                namespace_fn={'col':'support_talk_90_pre_treatment', 'fn': is_talk_page})

#@timeit
def add_support_talk_90_post_treatment(df):
    return create_and_merge_talk(df, start_date=sim_treatment_date, end_date=sim_experiment_end_date,
                                namespace_fn={'col':'support_talk_90_post_treatment', 'fn': is_talk_page})

#@timeit
def add_project_talk_90_pre_treatment(df):
    return create_and_merge_talk(df, start_date=sim_observation_start_date, end_date=sim_treatment_date,
                                namespace_fn={'col':'project_talk_90_pre_treatment', 'fn': is_wp_page})

#@timeit
def add_project_talk_90_post_treatment(df):
    return create_and_merge_talk(df, start_date=sim_treatment_date, end_date=sim_experiment_end_date,
                                namespace_fn={'col':'project_talk_90_post_treatment', 'fn': is_wp_page})




# ENCOURAGEMENT

class preloaded_csvs():
    def __init__(self):
        self.path_dfs = {}
        
    def get_csv(self, csv_path):
        try:
            return self.path_dfs[csv_path]
        except KeyError:
            grat_df = pd.read_csv(csv_path, usecols=['timestamp', 'sender_id'], parse_dates=[0])
            self.path_dfs[csv_path] = grat_df
            return grat_df
        

def get_num_grats(lang, user_id, user_df, start_date, end_date, grat_type, preloaded):
    cache_key = f'cache/{grat_type}/{lang}_{user_id}_{start_date}_{end_date}.pickle'
    if os.path.exists(cache_key):
        return pd.read_pickle(cache_key)
    else:
        # this could be optimized by keeping this in memory
        csv_path = os.path.join(GRAT_DIR, lang, 'outputs')
        lsdir = os.listdir(csv_path)
        try:
            grat_csv = [f for f in lsdir if grat_type in f][0]
            grat_csv_path = os.path.join(csv_path, grat_csv)
            grat_df = preloaded.get_csv(grat_csv_path)
            user_grats = grat_df[grat_df['sender_id']==user_id]
            user_grats = user_grats[(user_grats['timestamp']<end_date) & (user_grats['timestamp']>=start_date)]
            num_grats = len(user_grats)
        except IndexError: #occurs when language doesnt have wikilove
            num_grats = float('nan')
        col_name_suffix = 'pre' if start_date < sim_treatment_date else 'post'
        col_name = f'wiki{grat_type}_90_{col_name_suffix}_treatment'
        user_grat_df = pd.DataFrame.from_dict({col_name:[num_grats],
                                           'user_id':[user_id], 
                                           'lang':[lang]}, orient='columns')
        return user_grat_df

def create_grat_df(df, start_date, end_date, grat_type):
    count = 0
    grat_dfs = []
    preloaded = preloaded_csvs()
    for lang in lang_sqlparams.keys():
            user_ids = df[df['lang']==lang]['user_id'].values
            for user_id in user_ids:
#                 if count % 1000 == 0:
#                     print(f'Count {count} lang {lang} userid {user_id}')
                user_df = get_user_edits(lang, user_id, start_date, end_date)
                t0 = time.time()
                user_grat_df = get_num_grats(lang, user_id, user_df, start_date, end_date, grat_type, preloaded)
#                 print(f'user_id {user_id} took {time.time()-t0} to get reverts')
                grat_dfs.append(user_grat_df)
                count += 1
    grat_df = pd.concat(grat_dfs)
    return grat_df

def create_and_merge_encouragement(df, start_date, end_date, grat_type):
    grat_df = create_grat_df(df, start_date, end_date, grat_type)
    df = pd.merge(df, grat_df, how='left', on=['lang', 'user_id'])
    return df

#@timeit
def add_thanks_90_pre_treatment(df):
    return create_and_merge_encouragement(df, start_date=sim_observation_start_date, end_date=sim_treatment_date,
                                         grat_type='thank')

#@timeit
def add_thanks_90_post_treatment(df):
    return create_and_merge_encouragement(df, start_date=sim_treatment_date, end_date=sim_experiment_end_date,
                                         grat_type='thank')

#@timeit
def add_wikilove_90_pre_treatment(df):
    return create_and_merge_encouragement(df, start_date=sim_observation_start_date, end_date=sim_treatment_date,
                                         grat_type='love')

#@timeit
def add_wikilove_90_post_treatment(df):
    return create_and_merge_encouragement(df, start_date=sim_treatment_date, end_date=sim_experiment_end_date,
                                         grat_type='love')


#@timeit
def get_populations():
    lang_dfs = []

    for lang, sqlparams in lang_sqlparams.items():
        # print(f'Doing {lang}, with {sqlparams}.')
        lang_df = create_thanker_pop(lang, **sqlparams)
        lang_dfs.append(lang_df)

    df = pd.concat(lang_dfs)
    del lang_dfs
    return df



def make_data(subsample=None):
  print('starting to make data')
  print('making populations')
  df = get_populations()
  if subsample:
      print(f'subsetting to {subsample} samples')
      df = df.sample(n=subsample, random_state=1854)
  
  print('adding blocks')
  df = add_blocks_pre_treatment(df)
  df = add_blocks_post_treatment(df)
  # get user edits:
  cache_all_user_edits(df)
  
  print('adding reverts')
  df = add_revert_actions_pre_treatment(df)
  df = add_revert_actions_post_treatment(df)
  
  print('adding support talk')
  df = add_support_talk_90_pre_treatment(df)
  df = add_support_talk_90_post_treatment(df)
      
  print('adding project talk')
  df = add_project_talk_90_pre_treatment(df)
  df = add_project_talk_90_post_treatment(df)

  print('adding wikithanks')
  df = add_thanks_90_pre_treatment(df)
  df = add_thanks_90_post_treatment(df)
  
  
  print('adding wikiloves')
  df = add_wikilove_90_pre_treatment(df)
  df = add_wikilove_90_post_treatment(df)
  
  print('finished making data')
  return df

if __name__ == "__main__":
#     import subprocess
#     subprocess.Popen('scripts/wmf_ssh_tunnel.sh')
  conf = json.load(open('config/default.json','r'))
  subsample = conf['subsample'] if 'subsample' in conf.keys() else None
  GRAT_DIR = conf["GRAT_DIR"]
  df = make_data(conf['subsample'])
  df.to_csv(f'outputs/thanker_power_analysis_data_for_sim_treatment_{sim_treatment_date.strftime("%Y%m%d")}{("_"+str(subsample)+"_subsamples") if subsample else ""}.csv')

