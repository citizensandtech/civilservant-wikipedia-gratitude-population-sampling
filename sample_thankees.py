from wikipedia_helpers import to_wmftimestamp, from_wmftimestamp, decode_or_nan, make_wmf_con

import sys, os
import pandas as pd
from cached_df import make_cached_df

from datetime import datetime as dt
from datetime import timedelta as td


def sample_thankees_group_oriented(lang, db_con):
    """
        leaving this stub here because eventually i want to first start from what groups need
        :param lang:
        :param db_con:
        :return:
        """
    # load target group sizes
    # figure out which groups need more users

    # sample active users
    # remove users w/ < n edits
    # remove editors in


def get_active_users(lang, start_date, end_date, min_rev_id, wmf_con):
    """
    Return the first and last edits of only active users in `lang`wiki
    between the start_date and end_date.
    """
    raise NotImplementedError
    wmf_con.execute(f'use {lang}wiki_p;')
    active_sql = """select distinct(rev_user) from revision 
    where {start_date} <= rev_timestamp and rev_timestamp <= {end_date}
    and rev_id > {min_rev_id}
    ;
                """.format(start_date=to_wmftimestamp(start_date),
                           end_date=to_wmftimestamp(end_date),
                           lang=lang, min_rev_id=min_rev_id)
    active_df = pd.read_sql(active_sql, con)
    return active_df


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
    reg_sql = '''select '{lang}' as lang, user_id, user_name, user_registration,
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


def remove_inactive_users(df):
    """remove users who have no edits in the period"""
    return df[(pd.notnull(df['first_edit'])) | (pd.notnull(df['last_edit']))]


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
        user_ids = user_ids = df[df['lang'] == lang]['user_id'].values
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
    user_thank_sql = f"""
                    select thank_timestamp, sender, receiver, ru.user_id as receiver_id, su.user_id as sender_id from
                        (select log_timestamp as thank_timestamp, replace(log_title, '_', ' ') as receiver, log_user_text as sender
                        from logging_logindex where log_title = '{user_name.replace(' ', '')}'
                        and log_action = 'thank'
                        and {to_wmftimestamp(start_date)} <= log_timestamp <= {to_wmftimestamp(end_date)}) t
                    left join user ru on ru.user_name = t.receiver
                    left join user su on su.user_name = t.sender """
    df = pd.read_sql(user_thank_sql, wmf_con)
    df['thank_timestamp'] = df['thank_timestamp'].apply(from_wmftimestamp)
    df['sender'] = df['sender'].apply(decode_or_nan)
    df['receiver'] = df['receiver'].apply(decode_or_nan)
    return df


def add_thanks(df, start_date, end_date, col_name, wmf_con):
    user_thank_count_dfs = []
    for lang in langs:
        user_names = user_names = df[df['lang'] == lang]['user_name'].values
        for user_name in user_names:
            user_thank_df = get_thanks_thanking_user(lang, user_name, start_date, end_date, wmf_con)
            user_thank_count_df = pd.DataFrame.from_dict({col_name: [len(user_thank_df)],
                                                          'user_name': [user_name],
                                                          'lang': [lang]}, orient='columns')
            user_thank_count_dfs.append(user_thank_count_df)

    thank_counts_df = pd.concat(user_thank_count_dfs)
    df = pd.merge(df, thank_counts_df, how='left', on=['lang', 'user_name'])
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


def stratified_subsampler(df, sample_size):
    """
    take 100 from every group except newcomers
    :param df:
    :param sample_size: the number of samples per group
    :return: sum
    """
    from IPython import embed;
    embed()
    subsamples = []
    bin_groups = df.groupby(by=['lang', 'bin_name'])
    for (is_active, lang, bin_name), group in bin_groups:
        if bin_name == 'bin_0':
            sample_size = sample_size * 2
        if len(group) < sample_size:
            sample_size = len(group)
        subsample = group.sample(n=sample_size, random_state=1854)
        subsamples.append(subsample)

    return pd.concat(subsamples)

def make_data(subsample, wikipedia_start_date, sim_treatment_date, sim_observation_start_date, sim_experiment_end_date,
              wmf_con):
    print('starting to make data')

    df = make_populations(start_date=wikipedia_start_date, end_date=sim_treatment_date, wmf_con=wmf_con)
    df = remove_inactive_users(df)
    if not subsample:
        output_bin_stats(df)
    df = add_experience_bin(df)

    if subsample:
        print(f'subsetting to {subsample} samples')
        df = stratified_subsampler(df, subsample)

    df = add_thanks(df, start_date=sim_observation_start_date, end_date=sim_treatment_date,
                    col_name='num_thanks_received_pre_treatment', wmf_con=wmf_con)
    df = add_total_edits(df, start_date=wikipedia_start_date, end_date=sim_treatment_date, wmf_con=wmf_con)
    df = add_has_email_currently(df, wmf_con=wmf_con)

    return df


if __name__ == "__main__":
    subsample = os.getenv('subsample', 100)
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
