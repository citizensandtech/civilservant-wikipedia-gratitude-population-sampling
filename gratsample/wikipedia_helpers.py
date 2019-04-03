import os
from datetime import datetime as dt, timedelta as td
from sqlalchemy import create_engine
from itertools import islice

from sqlalchemy.orm import sessionmaker

from gratsample.orm_models import Base


def from_wmftimestamp(bytestring):
    if bytestring:
        s = bytestring.decode('utf-8')
        return dt.strptime(s, '%Y%m%d%H%M%S')
    else:
        return bytestring


def to_wmftimestamp(date):
    return date.strftime('%Y%m%d%H%M%S')


def decode_or_nan(b):
    return b.decode('utf-8') if b else float('nan')

def make_a_con(user, pwd, host, port):
    constr = 'mysql+pymysql://{user}:{pwd}@{host}:{port}/?charset=utf8'.format(user=user, pwd=pwd, host=host, port=port)

    con = create_engine(constr, encoding='utf-8')
    return con

def make_wmf_con():
    return make_a_con(user=os.environ['WMF_MYSQL_USERNAME'],
                      pwd=os.environ['WMF_MYSQL_PASSWORD'],
                      host=os.environ['WMF_MYSQL_HOST'],
                      port=os.environ['WMF_MYSQL_PORT'])

def load_session_from_con(con):
    Base.metadata.bind = con
    DBSession = sessionmaker(bind=con)
    db_session = DBSession()
    return db_session


def make_interal_con():
    return make_a_con(user=os.environ['LOCAL_MYSQL_USERNAME'],
                      pwd=os.environ['LOCAL_MYSQL_PASSWORD'],
                      host=os.environ['LOCAL_MYSQL_HOST'],
                      port=os.environ['LOCAL_MYSQL_PORT'])

def make_internal_db_session():
    return load_session_from_con(make_interal_con())


def make_sessions(ts_list):
    # these structures store the timestamps
    edit_sessions = []
    curr_edit_session = []

    # initialize prev to the earliest data possible
    prev_timestamp = dt(year=2001, month=1, day=1)

    for index, ts in enumerate(ts_list):
        #         print('index:', index)
        curr_timestamp = ts
        # if curr timestamp within 1 hour of last then append
        if curr_timestamp - prev_timestamp < td(hours=1):
            curr_edit_session.append(curr_timestamp)
        # else start a new edit session
        else:
            # if there's a pre-existing session save it to the return
            if curr_edit_session:
                edit_sessions.append(curr_edit_session)
            # and start a new session
            curr_edit_session = [curr_timestamp]
        # this is before
        if index < len(ts_list) - 1:
            prev_timestamp = curr_timestamp
        # this is the last item save this session too.
        else:
            #             print('this is the end')
            edit_sessions.append(curr_edit_session)

    return edit_sessions


def calc_labour_hours(ts_list):
    sessions = make_sessions(ts_list)
    total_labour_hours = 0
    for session in sessions:
        if len(session) == 1:
            total_labour_hours += 1
        else:
            session_duration = max(session) - min(session)
            session_seconds = session_duration.seconds
            session_hours = session_seconds / (60 * 60)
            session_hours += 1  # for this session
            total_labour_hours += session_hours
    return total_labour_hours


def ts_in_week(ts_list, date_start, date_end):
    in_week = []
    for ts in ts_list:
        if ts > date_start and ts <= date_end:
            in_week.append(ts)
    return in_week

def window_seq(seq, n=2):
    "Returns a sliding window (of width n) over data from the iterable"
    "   s -> (s0,s1,...s[n-1]), (s1,s2,...,sn), ...                   "
    it = iter(seq)
    result = tuple(islice(it, n))
    if len(result) == n:
        yield result
    for elem in it:
        result = result[1:] + (elem,)
        yield result


def namespace_all(ns):
    return True

def namespace_nontalk(ns):
    return ns %2 == 0

def namespace_mainonly(ns):
    return ns == 0

