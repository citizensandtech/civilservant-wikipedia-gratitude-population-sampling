import sqlalchemy

from gratsample.sample_thankees_revision_utils import num_quality_revisions, get_timestamps_within_range, \
    get_recent_edits_alias
from gratsample.wikipedia_helpers import to_wmftimestamp, from_wmftimestamp, decode_or_nan, make_wmf_con, calc_labour_hours, \
    ts_in_week, namespace_all, namespace_mainonly, namespace_nontalk

import os
import pandas as pd
from gratsample.cached_df import make_cached_df

from datetime import datetime as dt
from datetime import timedelta as td


def output_bin_stats(df):
    bin_stats = pd.DataFrame(df.groupby(['experience_level_pre_treatment', 'lang']).size()).rename(
        columns={0: 'users_with_at_least_days_experience'})
    bin_stats.to_csv('outputs/bin_stats_df_one_edit_min.csv', index=False)


@make_cached_df('spans')
def get_users_edit_spans(lang, start_date, end_date, wmf_con):
    """
    Return the the first and last edits of all users in `lang`wiki
    between the start_date and end_date
    """
    db_prefix = f'{lang}wiki_p'
    wmf_con.execute(f'use {db_prefix};')
    reg_sql = '''select '{lang}' as lang, user_id, user_name, user_registration, user_editcount as live_edit_count,
       (select min(rev_timestamp) from revision_userindex where rev_user=user_id and {start_date} <= rev_timestamp <= {end_date}) as first_edit, 
       (select max(rev_timestamp) from revision_userindex where rev_user=user_id and {start_date} <= rev_timestamp <= {end_date}) as last_edit
from user where coalesce(user_registration, 20010101000000) <= {end_date} 
     and 
                coalesce(user_registration, 20010101000000) >= {start_date};
'''.format(start_date=to_wmftimestamp(start_date),
           end_date=to_wmftimestamp(end_date),
           lang=lang)
    span_df = pd.read_sql(reg_sql, wmf_con)
    span_df['user_registration'] = span_df['user_registration'].apply(from_wmftimestamp)
    span_df['first_edit'] = span_df['first_edit'].apply(from_wmftimestamp)
    span_df['last_edit'] = span_df['last_edit'].apply(from_wmftimestamp)
    span_df['user_name'] = span_df['user_name'].apply(decode_or_nan)
    return span_df


def make_populations(start_date, end_date, wmf_con):
    """for every registered user get first and last edit (or not of those users didn't edit in the period)"""
    span_dfs = []
    for lang in langs:
        span_df = get_users_edit_spans(lang, start_date, end_date, wmf_con)
        span_dfs.append(span_df)
    return pd.concat(span_dfs)


def remove_inactive_users(df, start_date, end_date):
    """remove users who have no edits in the period
    :param start_date:
    :param end_date:
    """
    active_df = df[(pd.notnull(df['first_edit'])) | (pd.notnull(df['last_edit']))].copy()
    days_between = end_date - start_date
    active_df[f'active_in_{days_between.days}_pre_treatment'] = True
    return active_df

@make_cached_df('disablemail')
def get_user_disablemail_properties(lang, user_id, wmf_con):
    wmf_con.execute(f"use {lang}wiki_p;")
    user_prop_sql = f"""select * from user_properties where up_user = {user_id}
                        and up_property = 'disablemail';"""
    df = pd.read_sql(user_prop_sql, wmf_con)
    return df


def add_has_email_currently(df, wmf_con):
    user_prop_dfs = []
    for lang in langs:
        user_ids = df[df['lang'] == lang]['user_id'].values
        # print(f'{lang} has {len(user_ids)} disablemails to get')
        for user_id in user_ids:
            user_prop_df = get_user_disablemail_properties(lang, user_id, wmf_con)
            has_email = False if len(
                user_prop_df) >= 1 else True  # the property disables email, if it doesn't exist the default its that it's on
            user_prop_dfs.append(pd.DataFrame.from_dict({'has_email': [has_email],
                                                         'user_id': [user_id],
                                                         'lang': [lang]}, orient='columns'))

    users_prop_df = pd.concat(user_prop_dfs)
    df = pd.merge(df, users_prop_df, how='left', on=['lang', 'user_id'])
    return df


@make_cached_df('thanks')
def get_thanks_thanking_user(lang, user_name, start_date, end_date, wmf_con):
    wmf_con.execute(f"use {lang}wiki_p;")
    user_thank_sql = """
                    select thank_timestamp, sender, receiver, ru.user_id as receiver_id, su.user_id as sender_id from
                        (select log_timestamp as thank_timestamp, replace(log_title, '_', ' ') as receiver, log_user_text as sender
                        from logging_logindex where log_title = :user_name
                        and log_action = 'thank'
                        and :start_date <= log_timestamp <= :end_date ) t
                    left join user ru on ru.user_name = t.receiver
                    left join user su on su.user_name = t.sender """
    user_thank_sql_esc = sqlalchemy.text(user_thank_sql)
    sql_params = {'user_name': user_name.replace(' ', '_'), 'start_date':to_wmftimestamp(start_date), 'end_date':to_wmftimestamp(end_date)}
    df = pd.read_sql(user_thank_sql_esc, con=wmf_con, params=sql_params)
    df['thank_timestamp'] = df['thank_timestamp'].apply(from_wmftimestamp)
    df['sender'] = df['sender'].apply(decode_or_nan)
    df['receiver'] = df['receiver'].apply(decode_or_nan)
    return df


def add_thanks(df, start_date, end_date, col_name, wmf_con):
    user_thank_count_dfs = []
    for lang in langs:
        user_names = df[df['lang'] == lang]['user_name'].values
        for user_name in user_names:
            user_thank_df = get_thanks_thanking_user(lang, user_name, start_date, end_date, wmf_con)
            user_thank_count_df = pd.DataFrame.from_dict({col_name: [len(user_thank_df)],
                                                          'user_name': [user_name],
                                                          'lang': [lang]}, orient='columns')
            user_thank_count_dfs.append(user_thank_count_df)

    thank_counts_df = pd.concat(user_thank_count_dfs)
    df = pd.merge(df, thank_counts_df, how='left', on=['lang', 'user_name'])
    return df


def add_num_quality(df: object, col_name: object, namespace_fn: object, end_date: object, wmf_con: object) -> object:
    """note this get thes the number of quality revisions that are 90 days before users last edit before the end_date
    so, it's different than num_edits_90_pre_treatment because it could go farther back"""
    num_quality_dfs = []
    for lang in langs:
        user_ids = df[df['lang'] == lang]['user_id'].values
        for user_id in user_ids:
            # print(f'lang: {lang}, user_id: {user_id}')
            num_quality = num_quality_revisions(user_id=user_id, lang=lang, wmf_con=wmf_con, namespace_fn=namespace_fn,
                                                end_date=end_date)
            user_thank_count_df = pd.DataFrame.from_dict({col_name: [num_quality],
                                                          'user_id': [user_id],
                                                          'lang': [lang]}, orient='columns')
            num_quality_dfs.append(user_thank_count_df)

    quality_counts_df = pd.concat(num_quality_dfs)
    df = pd.merge(df, quality_counts_df, how='left', on=['lang', 'user_id'])
    return df


def add_edits_fn_by_week(df, col_name, wmf_con, timestamp_list_fn, edit_getter_fn=get_timestamps_within_range, start_date=None, end_date=None):
    for i in range(1, 13):
        week_col_name = f'{col_name}_week_{i}'
        week_col_name_any = f'{col_name}_week_{i}_any'
        df = add_edits_fn(df, week_col_name, wmf_con, timestamp_list_fn, edit_getter_fn=edit_getter_fn, start_date=start_date, end_date=end_date, week_number=i)
        df[week_col_name_any] = df[week_col_name].apply(lambda x: x>0)
    return df

def add_edits_fn(df, col_name, wmf_con, timestamp_list_fn, edit_getter_fn=get_timestamps_within_range, start_date=None, end_date=None, week_number=None):
    '''add the number of edits a user made within range'''
    edit_measure_dfs =[]
    for lang in langs:
        user_ids = df[df['lang'] == lang]['user_id'].values
        for user_id in user_ids:
            ts_series = edit_getter_fn(lang, user_id, wmf_con, start_date, end_date)
            ts_list = list(ts_series['rev_timestamp'])
            if week_number is not None:
                days_after_treat_start = week_number * 7
                days_after_treat_end =  (week_number+1)* 7
                week_start_date = start_date + td(days=days_after_treat_start)
                week_end_date = start_date + td(days=days_after_treat_end)

                ts_list = ts_in_week(ts_list, week_start_date, week_end_date)
            edit_measure_df = pd.DataFrame.from_dict({col_name: [timestamp_list_fn(ts_list)],
                                                          'user_id': [user_id],
                                                          'lang': [lang]}, orient='columns')
            edit_measure_dfs.append(edit_measure_df)

    edit_measures_df = pd.concat(edit_measure_dfs)
    df = pd.merge(df, edit_measures_df, how='left', on=['lang', 'user_id'])

    return df


def bin_from_td(delta):
    bins_log2 = (0, 90, 180, 365, 730, 1460, 2920, 5840)
    delta_days = delta.days
    prev_threshold = 0
    for threshold in bins_log2:
        if delta_days > threshold:
            prev_threshold = threshold
            continue
        else:
            break
    return f'bin_{prev_threshold}'


def add_experience_bin(df):
    df['days_since_registration'] = sim_treatment_date - df['user_registration']
    df['experience_level_pre_treatment'] = df['days_since_registration'].apply(bin_from_td)
    del df['days_since_registration']
    return df


@make_cached_df('total_edits')
def get_total_user_edits(lang, user_id, start_date, end_date, wmf_con):
    wmf_con.execute(f"use {lang}wiki_p;")
    user_edit_sql = f"""select count(*) as edits_pre_treatment from revision_userindex 
                where rev_user = {user_id} 
                and {to_wmftimestamp(start_date)} <= rev_timestamp <= {to_wmftimestamp(end_date)};
                """
    df = pd.read_sql(user_edit_sql, wmf_con)
    return df


def add_total_edits(df, start_date, end_date, wmf_con):
    user_edit_dfs = []
    for lang in langs:
        user_ids = user_ids = df[df['lang'] == lang]['user_id'].values
        for user_id in user_ids:
            user_edit_df = get_total_user_edits(lang, user_id, start_date, end_date, wmf_con)
            user_edit_df['user_id'] = user_id
            user_edit_df['lang'] = lang
            user_edit_dfs.append(user_edit_df)

    users_edit_df = pd.concat(user_edit_dfs)
    df = pd.merge(df, users_edit_df, how='left', on=['lang', 'user_id'])
    return df


def remove_with_min_edit_count(df, min_edit_count=4):
    """
    remove all the users with less than min_edit_count
    :param df:
    :param min_edit_count:
    :return:
    """
    return df[df['recent_edits_pre_treatment']>=min_edit_count]

def stratified_subsampler(df, sample_size, newcomer_multiplier=2):
    """
    take 100 from every group except newcomers
    :param df:
    :param sample_size: the number of samples per group
    :return: sum
    """
    subsamples = []
    bin_groups = df.groupby(by=['lang', 'experience_level_pre_treatment'])
    for (lang, bin_name), group in bin_groups:
        n_samp = sample_size
        if bin_name == 'bin_0':
            n_samp = sample_size * newcomer_multiplier
        if len(group) <= n_samp:
            n_samp = len(group)

        subsample = group.sample(n=n_samp, random_state=1854)
        subsamples.append(subsample)

    return pd.concat(subsamples)


def make_data(subsample, wikipedia_start_date, sim_treatment_date, sim_observation_start_date, sim_experiment_end_date,
              wmf_con):
    print('starting to make data')
    # embed()
    df = make_populations(start_date=wikipedia_start_date, end_date=sim_treatment_date, wmf_con=wmf_con)
    df = remove_inactive_users(df, start_date=sim_observation_start_date, end_date=sim_treatment_date)
    if not subsample:
        output_bin_stats(df)
    df = add_experience_bin(df)
    print('Simulated Active Editors')
    print(df.groupby(['lang','experience_level_pre_treatment']).size())
    if subsample:
        print(f'make a first reasonable subsample of {10*subsample} samples per group to be able to get their edit counts beforehand')
        print('this wouldnt be as big of a problem live because edit count is easy to get live')
        df = stratified_subsampler(df, 10*subsample, newcomer_multiplier=5)

    print('Random Stratified Subsample of active Editors to get edit counts with last 90')
    print(df.groupby(['lang','experience_level_pre_treatment']).size())

    df = add_edits_fn(df, col_name='recent_edits_pre_treatment', timestamp_list_fn=len, edit_getter_fn=get_recent_edits_alias, wmf_con=wmf_con)
    df = remove_with_min_edit_count(df, min_edit_count=4) #  in the future remove this step by just including edit_count from the make_populations step
    print('Random Stratified Subsample Having min 4 edits in the last 90')
    print(df.groupby(['lang','experience_level_pre_treatment']).size())
    if subsample:
        print(f'subsetting to {subsample} samples')
        df = stratified_subsampler(df, subsample)

    print('Second Random Stratified subsample to Get Edit Quality Data')
    print(df.groupby(['lang','experience_level_pre_treatment']).size())

    print("adding thanks")
    df = add_thanks(df, start_date=sim_observation_start_date, end_date=sim_treatment_date,
                    col_name='num_prev_thanks_in_90_pre_treatment', wmf_con=wmf_con)

    print("adding thanks")
    df = add_total_edits(df, start_date=wikipedia_start_date, end_date=sim_treatment_date, wmf_con=wmf_con)
    print("adding email")
    df = add_has_email_currently(df, wmf_con=wmf_con)

    print("adding quality")
    df = add_num_quality(df, col_name='num_quality_pre_treatment', wmf_con=wmf_con, namespace_fn=namespace_all, end_date=sim_treatment_date)
    print("adding quality nontalk")
    df = add_num_quality(df, col_name='num_quality_pre_treatment_non_talk', namespace_fn=namespace_nontalk, end_date=sim_treatment_date, wmf_con=wmf_con)
    print("adding quality main only")
    df = add_num_quality(df, col_name='num_quality_pre_treatment_main_only', namespace_fn=namespace_mainonly, end_date=sim_treatment_date, wmf_con=wmf_con)

    print('adding 90 pre treatment')
    df = add_edits_fn(df, col_name='num_edits_90_pre_treatment', wmf_con=wmf_con, start_date=sim_observation_start_date,
                      end_date=sim_treatment_date, timestamp_list_fn=len)
    print('adding 90 post treatment')
    df = add_edits_fn(df, col_name='num_edits_90_post_treatment', wmf_con=wmf_con, start_date=sim_treatment_date,
                      end_date=sim_experiment_end_date, timestamp_list_fn=len)
    print('adding laborhours pre treatment')
    df = add_edits_fn(df, col_name='num_labor_hours_90_pre_treatment', wmf_con=wmf_con, start_date=sim_observation_start_date,
                      end_date=sim_treatment_date, timestamp_list_fn=calc_labour_hours)
    print('adding laborhours post treatment')
    df = add_edits_fn(df, col_name='num_labor_hours_90_post_treatment', wmf_con=wmf_con, start_date=sim_treatment_date,
                      end_date=sim_experiment_end_date, timestamp_list_fn=calc_labour_hours)

    print('adding 90 post treatment by week')
    df = add_edits_fn_by_week(df, col_name='num_edits_90_post_treatment', wmf_con=wmf_con, start_date=sim_treatment_date,
                      end_date=sim_experiment_end_date, timestamp_list_fn=len)
    print('adding laborhours post treatment')
    df = add_edits_fn_by_week(df, col_name='num_labor_hours_90_post_treatment', wmf_con=wmf_con, start_date=sim_treatment_date,
                      end_date=sim_experiment_end_date, timestamp_list_fn=calc_labour_hours)


    print('done')
    return df


if __name__ == "__main__":
    subsample = int(os.getenv('subsample', 10))
    langs = [lang for lang in os.getenv('LANGS').split(',')]
    treatment_date_parts = [int(timepart) for timepart in os.getenv('TREATMENT_DATE').split(',')]

    sim_treatment_date = dt(*treatment_date_parts)
    sim_experiment_end_date = sim_treatment_date + td(days=90)
    sim_observation_start_date = sim_treatment_date - td(days=90)
    wikipedia_start_date = dt(2002, 1, 1)
    wmf_con = make_wmf_con()
    df = make_data(subsample=subsample, wikipedia_start_date=wikipedia_start_date,
                   sim_treatment_date=sim_treatment_date,
                   sim_observation_start_date=sim_observation_start_date,
                   sim_experiment_end_date=sim_experiment_end_date,
                   wmf_con=wmf_con)
    today_str = dt.today().strftime('%Y%m%d')
    out_fname =   f'outputs/thankee_power_analysis_data_for_sim_treatment_{sim_treatment_date.strftime("%Y%m%d")}{("_"+str(subsample)+"_subsamples") if subsample else ""}.csv'
    df.to_csv(os.path.join('outputs', out_fname))
