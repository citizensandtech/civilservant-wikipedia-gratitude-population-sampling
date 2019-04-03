import json
import os
from unittest.mock import patch
import pytest
import pandas as pd
from gratsample.sample_thankees_revision_utils import num_quality_revisions, get_display_data


def load_path_files_to_dict(sub_dirname, filetype):
    sub_dir = os.path.join('test_data', sub_dirname)
    reader_fn = {'.json': lambda f: json.load(open(os.path.join(sub_dir, f), 'r')),
               '.csv': lambda f: pd.read_csv(open(os.path.join(sub_dir, f), 'r'))}
    reader = reader_fn[filetype]
    fname_file = {f: reader(f) for f in os.listdir(sub_dir) if f.endswith(filetype)}
    return fname_file

@pytest.fixture
def display_data():
    return load_path_files_to_dict('display_data','.json')

@pytest.fixture
def mwapi_responses():
    return load_path_files_to_dict('mwapi_responses','.json')

@pytest.fixture
def oresapi_responses():
    return load_path_files_to_dict('ores_api_responses', '.json')

@pytest.fixture
def wmf_con_responses():
    return load_path_files_to_dict('con_responses','.csv')

@patch('ores_api.Session.score')
@patch('pandas.read_sql')
def test_num_quality_revisions_ores(mock_con, mock_ores_session_score, oresapi_responses, wmf_con_responses):
    user_id = 697036
    lang = "fa"
    mock_con.return_value = wmf_con_responses[f'get_recent_edits_{lang}_{user_id}.csv']
    mock_ores_session_score.return_value = oresapi_responses[f'ores_api_{lang}_{21736707}.json']
    assert num_quality_revisions(697036, 'fa') == 0
    # num_quality_revisions(2760580, 'de') ==5

@patch('mwapi.Session.get')
def test_get_display_data(mock_mwapi_session, display_data, mwapi_responses):
    mock_mwapi_session.side_effect = [mwapi_responses['r0.json'], mwapi_responses['r1.json'], mwapi_responses['r3.json'], mwapi_responses['r4.json']]
    assert get_display_data([32932453, 32745075], 'ar') == display_data['display_data_ar_2.json']

