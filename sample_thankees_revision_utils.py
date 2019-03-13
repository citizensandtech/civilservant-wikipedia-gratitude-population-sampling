# coding: utf-8
import datetime
import json
import requests

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

from wikipedia_helpers import make_wmf_con


CACHE_ROOT = os.getenv('CACHE_DIR', './cache')
GRAT_ROOT = os.getenv('GRAT_DIR', '../gratitude/outputs/')

# In[130]:


import mwreverts.api
import mwapi


def get_revisions_and_flagged_data(rev_ids, treatment_date):
    """get number of revisons flagged before `treatment_date`
      among last 50 edits"""
    rev_flag_sql = """
    select rev_id, 
            rev_page, 
            page_namespace, 
            rev_timestamp, 
            fr_timestamp, 
            (select max(fr_timestamp) from flaggedrevs where fr_page_id=rev_page and fr_timestamp < {treatment_date}) max_fr_ts 
        from (
              select rev_id, rev_page, rev_timestamp, page_namespace from revision_userindex
                join page on page_id = rev_page where rev_id in ({rev_ids_str}) ) auser
        left join flaggedrevs on
            fr_page_id = rev_page and
            fr_rev_id = rev_id;
                """.format(rev_ids_str=','.join([str(x) for x in rev_ids]),
                           treatment_date=to_wmftimestamp(treatment_date))

    con.execute('use dewiki_p;')
    rev_flag = pd.read_sql(rev_flag_sql, con)
    rev_flag['fr_timestamp'] = rev_flag['fr_timestamp'].apply(from_wmftimestamp)
    rev_flag['max_fr_ts'] = rev_flag['max_fr_ts'].apply(from_wmftimestamp)
    rev_flag['rev_timestamp'] = rev_flag['rev_timestamp'].apply(from_wmftimestamp)
    return rev_flag


def was_reverted(rev_id):
    try:
        _, reverted, reverted_to = mwreverts.api.check(
            de_session, rev_id, radius=3,  # most reverts within 5 edits
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


def get_flagged_decision_df(rev_ids, treatment_date):
    rev_df = get_revisions_and_flagged_data(rev_ids, treatment_date)
    rev_df['was_reverted'] = rev_df.apply(
        lambda row: was_reverted(row['rev_id']) if pd.isnull(row['fr_timestamp']) else 'no_check', axis=1)
    rev_df['flagged'] = rev_df.apply(decide_flagged, axis=1)
    return rev_df


def get_flagged_revs(rev_ids, treatment_date):
    needed_columns = ['user_id', 'rev_id', 'rev_timestamp', 'was_flagged', 'was_reverted']
    rev_df = get_flagged_decision_df(rev_ids, treatment_date)
    rev_df['quality_enough'] = rev_df.apply(decide_flagged, axis=1)
    return rev_df


# # Get Revisions of Editors


def get_recent_edits(lang, user_id, con, prior_days=84, max_revs=50):
    '''this will get all the rev_ids for a user that occured less than `prior_days` days before their last edit
    and no more than `max_revs` edits in total
    :param con: '''
    con.execute('use {lang}wiki_p;'.format(lang=lang))
    revsql = '''
            select user_id, ts as rev_timestamp, rev_id from
            (select a.rev_user as user_id, timestamp(a.rev_timestamp) as ts, a.rev_id as rev_id, timestamp(b.mts) as mts
            from
            (select rev_user, rev_timestamp, rev_id from revision_userindex where rev_user = {user_id}) a
            join
            (select rev_user, max(rev_timestamp) as mts from revision_userindex where rev_user = {user_id})  b
            on a.rev_user = b.rev_user
            ) uhist
            where ts > date_sub(mts, interval {prior_days} day)
            limit {max_revs};'''.format(user_id=user_id, prior_days=prior_days, max_revs=max_revs)
    udf = pd.read_sql(revsql, con)
    return udf


def get_all_users_revs(refresh_users, lang, wmf_con):
    """
    of the users needing refresh, get their their recent edits
    refresh_users is a DF having columns lang and user_id
    """
    all_users_revs_dfs = []
    for user_id in refresh_users['user_id'].values:
        recent_users_revs = get_recent_edits(lang, user_id, wmf_con)
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


def ores_quality_getter(rev_ids, context_lang):
    scores = get_ores_data_dgf_from_api(rev_ids, context_lang)
    print('predictions_ndgf:', scores)
    #     print([s for s in scores])
    try:
        predictions = [(sc['damaging']['score']['prediction'], sc['goodfaith']['score']['prediction']) for sc in scores]
        #         print(predictions)
        predictions_ndgf = [(not (d) and gf) for (d, gf) in predictions]
    #         print(predictions_ndgf)
    except KeyError:  # probably couldn't get the score for because it doesn't exist
        predictions_ndgf = [False for i in range(len(rev_ids))]
    #     rev_ids_scores = dict(zip(rev_ids, predictions_ndgf))
    rev_ids_scores = pd.DataFrame.from_dict({'rev_id': rev_ids, 'quality_enough': predictions_ndgf}, orient='columns')
    rev_ids_scores['lang'] = context_lang
    return rev_ids_scores


def flagged_rev_quality_getter(rev_ids, context_lang):
    rev_ids_flagged = get_flagged_revs(rev_ids, treatment_date=datetime.datetime.utcnow())
    rev_ids_flagged['lang'] = context_lang
    return rev_ids_flagged


def remove_non_quality_revs(all_user_revs):
    """
    remove the revisions that aren't ORES good faith for ar/pl/fa
    or aren't flagged for de.
    """
    langs = list(set(all_user_revs['lang']))
    rev_ids = all_user_revs['rev_id'].values

    if len(langs) > 1:
        raise NotImplementedError('Please just process one language at a time for sanity sake')
    elif langs[0] in ['ar', 'pl', 'fa']:
        revs_quality = ores_quality_getter(rev_ids, langs[0])
    elif langs[0] in ['de']:
        revs_quality = flagged_rev_quality_getter(rev_ids, 'de')

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


def get_quality_edits_of_users(refresh_users, lang, wmf_con):
    """get all the quality edits of refresh_users stored or live"""
    all_user_revs = get_all_users_revs(refresh_users, lang, wmf_con)
    quality_user_revs = remove_non_quality_revs(all_user_revs)
    return quality_user_revs


def num_quality_revisions(user_id, lang):
    """report the number of quality revisions a user has"""
    refresh_user = pd.DataFrame({'user_id': [user_id]})
    wmf_con = make_wmf_con()
    all_user_revs = get_quality_edits_of_users(refresh_user, lang, wmf_con)
    return len(all_user_revs)


def refresh_revisions(refresh_users, lang):
    """assumption we are only refreshing users who are known to need refresh.
    we assume that another process calculates who needs refersh based on their edit count.
    In additon just doing 1 `lang` at a time. So a calling function would have to loop over langs"""
    # do this in a user-oriented way, or a process-oriented way?
    # revisions of users
    all_user_revs = get_quality_edits_of_users(refresh_users, lang)
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
