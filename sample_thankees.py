from wikipedia_helpers import to_wmftimestamp, from_wmftimestamp, decode_or_nan, make_wmf_con

import json
import requests

from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
from pymysql.err import InternalError, OperationalError
import sys, os
import pandas as pd

import mwclient

from datetime import datetime as dt
from datetime import timedelta as td

langs = [lang for lang in os.getenv('LANGS').split(',')]



def sample_thankees(lang, db_con ):
    # load target group sizes
    # figure out which groups need more users

    # sample active users
    # remove users w/ < n edits
    # remove editors in

treatment_date_parts = [timepart for timepart in os.getenv('TREATMENT_DATE').split(',')]

sim_treatment_date = dt(*treatment_date_parts)
sim_experiment_end_date = sim_treatment_date + td(days=90)

sim_observation_start_date = sim_treatment_date - td(days=90)
con = make_wmf_con()

