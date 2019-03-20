# coding: utf-8
import datetime
import json
import requests
import sqlalchemy

from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
from pymysql.err import InternalError, OperationalError
import sys
import os
import pandas as pd
import numpy as np

import mwclient
import mwviews
import mwapi
import ores_api

from datetime import datetime as dt
from datetime import timedelta as td

from cached_df import make_cached_df
from wikipedia_helpers import make_wmf_con, to_wmftimestamp, from_wmftimestamp

CACHE_ROOT = os.getenv('CACHE_DIR', './cache')
GRAT_ROOT = os.getenv('GRAT_DIR', '../gratitude/outputs/')

# In[130]:


import mwreverts.api
import mwapi


def get_revisions_and_flagged_data(rev_ids, treatment_date, con):
    """get number of revisons flagged before `treatment_date`
      among last 50 edits"""
    rev_flag_sql = """
    select rev_id, 
            rev_page, 
            page_namespace, 
            rev_timestamp, 
            fr_timestamp, 
            (select max(fr_timestamp) from flaggedrevs where fr_page_id=rev_page and fr_timestamp < :treatment_date) max_fr_ts 
        from (
              select rev_id, rev_page, rev_timestamp, page_namespace from revision_userindex
                join page on page_id = rev_page where rev_id in ({rev_ids_str}) ) auser
        left join flaggedrevs on
            fr_page_id = rev_page and
            fr_rev_id = rev_id;
                """.format(rev_ids_str="{}".format(','.join([str(x) for x in rev_ids])) if len(rev_ids)>0 else "null")
    rev_flag_params = {
                       'treatment_date': to_wmftimestamp(treatment_date)}
    # print(rev_flag_params)
    con.execute('use dewiki_p;')
    rev_flag = pd.read_sql(sqlalchemy.text(rev_flag_sql), con, params=rev_flag_params)
    rev_flag['fr_timestamp'] = rev_flag['fr_timestamp'].apply(from_wmftimestamp)
    rev_flag['max_fr_ts'] = rev_flag['max_fr_ts'].apply(from_wmftimestamp)
    rev_flag['rev_timestamp'] = rev_flag['rev_timestamp'].apply(from_wmftimestamp)
    return rev_flag


def was_reverted(rev_id, mwapi_session_de):
    try:
        _, reverted, reverted_to = mwreverts.api.check(
            mwapi_session_de, rev_id, radius=3,  # most reverts within 5 edits
            window=48 * 60 * 60,  # 2 days
            rvprop={'user', 'ids'})  # Some properties we'll make use of
        return True if reverted else False
    except (KeyError, mwapi.session.APIError) as err:
        print('Error getting revert status for {rev_id}'.format(rev_id=rev_id))
        return True  # because even if it was deleted from the DB for our purposes its still a bad edit


def decide_flagged(row):
    """Was this revision flagged (or generally high quality)?"""
    namespace = row['page_namespace']
    rev_time = row['rev_timestamp']
    flagged_time = row['fr_timestamp']
    last_flagged_time = row['max_fr_ts']
    was_reverted = row['was_reverted']

    # namespace check
    if namespace != 0:
        return True  # because often user-page edits are never approved
    # check if explictly flagged
    elif pd.notnull(flagged_time):
        return True
    # check if reverted
    elif was_reverted:
        return False
    # check if the last flagged time is after the edit
    elif last_flagged_time:
        if rev_time < last_flagged_time:
            return True
        # the revision exists but it hasn't been flagged yet.
        else:
            return False
    else:
        # not sure what else would get to this stage , but...
        return False


def get_flagged_decision_df(rev_ids, treatment_date, con):
    rev_df = get_revisions_and_flagged_data(rev_ids, treatment_date, con)
    if len(rev_df)==0:
        return rev_df
    mwapi_session_de = mwapi.Session('https://de.wikipedia.org',
        user_agent='CivilServant Experiment Sampler <max.klein@civilservant.io>')
    rev_df['was_reverted'] = rev_df.apply(
        lambda row: was_reverted(row['rev_id'], mwapi_session_de) if pd.isnull(row['fr_timestamp']) else 'no_check', axis=1)
    rev_df['flagged'] = rev_df.apply(decide_flagged, axis=1)
    return rev_df


def get_flagged_revs(rev_ids, treatment_date, con):
    needed_columns = ['user_id', 'rev_id', 'rev_timestamp', 'was_flagged', 'was_reverted']
    rev_df = get_flagged_decision_df(rev_ids, treatment_date, con)
    rev_df['quality_enough'] = rev_df.apply(decide_flagged, axis=1)
    return rev_df


# Get Revisions of Editors
@make_cached_df('timestamps')
def get_timestamps_within_range(lang, user_id, con, start_date, end_date):
    '''this will get all the timestamps of edits for a user that occured before or after 90 within a
    date range from start_date to end_date'''

    con.execute('use {lang}wiki_p;'.format(lang=lang))
    rev_sql = '''select rev_timestamp from revision_userindex where rev_user = :user_id
                and rev_timestamp >= :start_date and rev_timestamp < :end_date 
                order by rev_timestamp
                '''
    rev_sql_esc = sqlalchemy.text(rev_sql)
    sql_params = {'user_id': int(user_id), 'start_date': to_wmftimestamp(start_date), 'end_date': to_wmftimestamp(end_date)}
    rev_ts_series = pd.read_sql(rev_sql_esc, con=con, params=sql_params)
    rev_ts_series['rev_timestamp'] = rev_ts_series['rev_timestamp'].apply(from_wmftimestamp)
    return rev_ts_series

@make_cached_df('recent_edits')
def get_recent_edits_alias(lang, user_id, con, prior_days=None, max_revs=None, end_date=None):
    return get_recent_edits(lang, user_id, con, prior_days=None, max_revs=None, end_date=None)


def get_recent_edits(lang, user_id, con, prior_days=None, max_revs=None, end_date=None):
    '''this will get all the rev_ids for a user that occured less than `prior_days` days before their last edit before `start_date`
    and no more than `max_revs` edits in total
    :param con:
    :param start_date'''
    if not end_date:
        end_date = datetime.datetime.utcnow()
    if not prior_days:
        prior_days = 84
    if not max_revs:
        max_revs = 50
    con.execute('use {lang}wiki_p;'.format(lang=lang))
    revsql = ''' select user_id, rev_timestamp, rev_id, page_id, page_namespace from
            (select user_id, ts as rev_timestamp, rev_id, rev_page from
            (select a.rev_user as user_id, timestamp(a.rev_timestamp) as ts, a.rev_id as rev_id, timestamp(b.mts) as mts, rev_page
            from
            (select rev_user, rev_timestamp, rev_id, rev_page from revision_userindex where rev_user = {user_id} and rev_timestamp <= {end_date}) a
            join
            (select rev_user, max(rev_timestamp) as mts from revision_userindex where rev_user = {user_id} and rev_timestamp <= {end_date})  b
            on a.rev_user = b.rev_user
            ) uhist
            where ts > date_sub(mts, interval {prior_days} day)
            limit {max_revs}) revs
            join page
            on rev_page = page_id;
            '''.format(user_id=user_id, prior_days=prior_days, max_revs=max_revs, end_date=to_wmftimestamp(end_date))
    udf = pd.read_sql(revsql, con)
    return udf


def get_all_users_revs(refresh_users, lang, wmf_con, end_date):
    """
    of the users needing refresh, get their their recent edits
    refresh_users is a DF having columns lang and user_id
    """
    all_users_revs_dfs = []
    for user_id in refresh_users['user_id'].values:
        recent_users_revs = get_recent_edits(lang=lang, user_id=user_id, con=wmf_con, end_date=end_date)
        recent_users_revs['lang'] = lang
        all_users_revs_dfs.append(recent_users_revs)

    all_users_revs = pd.concat(all_users_revs_dfs)
    return all_users_revs


def get_ores_data_dgf_from_api(rev_ids, context_lang):
    session = ores_api.Session(
        'https://ores.wikimedia.org',
        user_agent='CivilServant Experiment Sampler <max.klein@civilservant.io>',
        batch_size=50,
        parallel_requests=4,
        retries=2)

    context = f'{context_lang}wiki'
    return session.score(context, ('damaging', 'goodfaith'), rev_ids)

@make_cached_df('ores_ndgf')
def ores_quality_getter(rev_ids, context_lang):
    # print(rev_ids)
    scores = get_ores_data_dgf_from_api(rev_ids, context_lang)
    # print([s for s in scores])
    try:
        predictions = [(sc['damaging']['score']['prediction'], sc['goodfaith']['score']['prediction']) for sc in scores]
        # print(predictions)
        predictions_ndgf = [(not (d) and gf) for (d, gf) in predictions]
        # print(predictions_ndgf)
    except KeyError:  # probably couldn't get the score for because it doesn't exist
        predictions_ndgf = [False for i in range(len(rev_ids))]
    #     rev_ids_scores = dict(zip(rev_ids, predictions_ndgf))
    rev_ids_scores = pd.DataFrame.from_dict({'rev_id': rev_ids, 'quality_enough': predictions_ndgf}, orient='columns')
    rev_ids_scores['lang'] = context_lang
    return rev_ids_scores


def flagged_rev_quality_getter(rev_ids, context_lang, con, treatment_date=datetime.datetime.utcnow()):
    rev_ids_flagged = get_flagged_revs(rev_ids, treatment_date=treatment_date, con=con)
    rev_ids_flagged['lang'] = context_lang
    return rev_ids_flagged


def remove_non_quality_revs(all_user_revs, lang, wmf_con, end_date):
    """
    remove the revisions that aren't ORES good faith for ar/pl/fa
    or aren't flagged for de.
    """
    rev_ids = all_user_revs['rev_id'].values
    # print(rev_ids)
    if len(rev_ids) == 0:
        # shortcircuit
        print("somehow there are no revs to check")
        return all_user_revs
    if lang in ['ar', 'pl', 'fa']:
        revs_quality = ores_quality_getter(rev_ids, lang)
    elif lang in ['de']:
        revs_quality = flagged_rev_quality_getter(rev_ids, 'de', wmf_con, treatment_date=end_date)

    all_user_revs_quality = pd.merge(all_user_revs, revs_quality, how="left", on=['rev_id', 'lang'])
    quality_user_revs = all_user_revs_quality[all_user_revs_quality['quality_enough'] == True]
    print(f'started with {len(all_user_revs)} revs, removed {len(all_user_revs)-len(quality_user_revs)}')
    return quality_user_revs


def get_diff_html_dict(rev_id, mwapi_session):
    ret = mwapi_session.get(action='compare', fromrev=rev_id, torelative='prev', prop='diff|user|comment|rel|title|ids')
    return ret['compare']


def get_rev_dict(old_rev_id, new_rev_id, mwapi_session):
    # check case where old_id_doesnt exist because its the newest edit on page
    revids_str = f'{old_rev_id}|{new_rev_id}' if old_rev_id else new_rev_id
    ret = mwapi_session.get(revids=revids_str, action='query', prop='revisions', rvprop='timestamp')
    page_key = list(ret['query']['pages'].keys())[0]
    rev_data = ret['query']['pages'][page_key]['revisions']
    if old_rev_id:
        return rev_data
    else:
        ({'timestamp': None}, rev_data[0])


def get_display_data(rev_ids, lang):
    display_data = []
    mwapi_session = mwapi.Session(host=f'https://{lang}.wikipedia.org',
                                  user_agent='civilservant datagathering <max@notconfusing.com>')
    for rev_id in rev_ids:
        try:
            diff = get_diff_html_dict(rev_id=rev_id, mwapi_session=mwapi_session)
            try:
                old_rev_id = diff['fromrevid']
            except KeyError:  # this is the very first edit on the page
                old_rev_id = None
            old_rev_data, new_rev_data = get_rev_dict(old_rev_id=old_rev_id, new_rev_id=rev_id,
                                                      mwapi_session=mwapi_session)
            display_datum = {'editDeleted': False, 'diffHTML': diff['*'], 'lang': lang,
                             'newRevId': rev_id, 'newRevDate': new_rev_data['timestamp'],
                             'newRevUser': diff['touser'], 'newRevComment': diff['toparsedcomment'],
                             'oldRevId': diff['fromrevid'], 'oldRevDate': old_rev_data['timestamp'],
                             'oldRevUser': diff['fromuser'], 'oldRevComment': diff['fromparsedcomment'],
                             'pageTitle': diff['totitle']}
        except mwapi.errors.APIError:
            display_datum = {'editDeleted': True, 'diffHTML': None, 'lang': lang,
                             'newRevId': None, 'newRevDate': None, 'newRevUser': None, 'newRevComment': None,
                             'oldRevId': None, 'oldRevDate': None, 'oldRevUser': None, 'oldRevComment': None,
                             'pageTitle': None}
        display_data.append(display_datum)

    return display_data

@make_cached_df('qualityedits')
def get_quality_edits_of_users(refresh_users, lang, wmf_con, namespace_fn=None, end_date=None):
    """get all the quality edits of refresh_users that are 90 days before their last stored or live"""
    all_user_revs = get_all_users_revs(refresh_users, lang, wmf_con, end_date)
    # from IPython import embed; embed()
    if namespace_fn and len(all_user_revs) > 0:
        # subset df based on user_revs
        all_user_revs = all_user_revs[all_user_revs['page_namespace'].apply(namespace_fn)]
    quality_user_revs = remove_non_quality_revs(all_user_revs, lang, wmf_con, end_date=end_date)
    return quality_user_revs


def num_quality_revisions(user_id, lang, wmf_con=None, namespace_fn=None, end_date=None):
    """report the number of quality revisions a user has"""
    refresh_user = pd.DataFrame({'user_id': [user_id]})
    if not wmf_con:
        wmf_con = make_wmf_con()
    all_user_revs = get_quality_edits_of_users(refresh_user, lang, wmf_con, namespace_fn=namespace_fn, end_date=end_date)
    return len(all_user_revs)


def refresh_revisions(refresh_users, lang, con):
    """assumption we are only refreshing users who are known to need refresh.
    we assume that another process calculates who needs refersh based on their edit count.
    In additon just doing 1 `lang` at a time. So a calling function would have to loop over langs"""
    # do this in a user-oriented way, or a process-oriented way?
    # revisions of users
    all_user_revs = get_quality_edits_of_users(refresh_users, lang, con)
    # already recevied revisions
    already_revs = get_already_revs(lang)
    # revisions needing getting = revs - already
    revs_to_get = set_subtract(all_user_revs, already_revs)
    # get and store.
    display_data = get_display_data(revs_to_get, lang)

    # return explictly the display data that would need to be sync'd back to server
    return display_data

if __name__=='__main__':
    con=make_wmf_con()
