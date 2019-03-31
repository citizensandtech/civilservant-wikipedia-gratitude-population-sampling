import sqlalchemy
import yaml

from sample_thankees import make_populations, remove_inactive_users, add_experience_bin, add_edits_fn, \
    remove_with_min_edit_count, add_thanks, add_total_edits, add_has_email_currently, add_num_quality
from sample_thankees_revision_utils import num_quality_revisions, get_timestamps_within_range, get_recent_edits, \
    get_recent_edits_alias
from wikipedia_helpers import to_wmftimestamp, from_wmftimestamp, decode_or_nan, make_wmf_con, calc_labour_hours, \
    ts_in_week, window_seq, namespace_all, namespace_mainonly, namespace_nontalk

import sys, os
import pandas as pd
from cached_df import make_cached_df

from datetime import datetime as dt
from datetime import timedelta as td
from IPython import embed


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




def make_data(subsample, wikipedia_start_date, sim_treatment_date, sim_observation_start_date, sim_experiment_end_date,
              wmf_con):
    print('starting to make data')
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

    print("adding email")
    df = add_has_email_currently(df, wmf_con=wmf_con)

    print("adding quality")
    df = add_num_quality(df, col_name='num_quality_pre_treatment', wmf_con=wmf_con, namespace_fn=namespace_all, end_date=sim_treatment_date)
    print("adding quality nontalk")
    df = add_num_quality(df, col_name='num_quality_pre_treatment_non_talk', namespace_fn=namespace_nontalk, end_date=sim_treatment_date, wmf_con=wmf_con)
    print("adding quality main only")
    df = add_num_quality(df, col_name='num_quality_pre_treatment_main_only', namespace_fn=namespace_mainonly, end_date=sim_treatment_date, wmf_con=wmf_con)



    print('done')
    return df

class thankeeOnboarder():
    def __init__(self, config_file):
        """groups needing edits and size N edits to be included which k edits to be displayed
        """
        config = yaml.load(open(os.path.join('config', config_file), 'r'))
        self.groups = config['groups']
        self.wmf_con = make_wmf_con()


    def sample_populations_per_language(self):
        """
        - sample active users
        - remove users with less than n edits
        - remove editors in thanker experiment
        - assign experince level (once only)
        - update/insert candidates
        - iterative representative sampling
        - add thanks history
        - add emailable status
        """

    def iterative_representative_sampling(self):
        """
        - iterating over groups that need more users
        - until reach target group size or out of candidates
        - for randomnly-ordered unincluded user in group
        - get and store edit quality user
        :return: includable-predicate
        """

    def refresh_edits_per_language(self):
        """
        - for includable user in group
        :return:
        """

    def refresh_edits(self, user, lang):
        """
        - update last k edits of included user
        - determine quality of edits
        - get editDisplay data
        :param user:
        :param lang:
        :return:
        """

    def send_included_users_to_cs_hq(self):
        """
        - send newly included users to cs hq
        - send new editdisplay data back to cs hq
        - over api
        :return:
        """

    def receive_active_uncompleted_users(self):
        """
        - for users that are in the experiment get from backend whether they still need refreshing
        :return:
        """

    def receive_users_in_thanker_experiment(self):
        """
        - may only be once , but need to know who is in the thanker expirement.
        :return:
        """

    def run(self):
        self.receive_active_uncompleted_users()
        self.receive_users_in_thanker_experiment()
        self.sample_populations_per_language()
        self.refresh_edits_per_language()
        self.send_included_users_to_cs_hq()

if __name__ == "__main__":
    config_file = os.getenv('ONBOARDER_CONFIG', 'onboarder.yaml')
    onboarder = thankeeOnboarder(config_file)
    onboarder.run()
