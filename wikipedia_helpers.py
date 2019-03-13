import os
from datetime import datetime as dt
from datetime import timedelta as td
from sqlalchemy import create_engine


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


def make_wmf_con():
    constr = 'mysql+pymysql://{user}:{pwd}@{host}:{port}/?charset=utf8'.format(user=os.environ['MYSQL_USERNAME'],
                                                                          pwd=os.environ['MYSQL_PASSWORD'],
                                                                          host=os.environ['MYSQL_HOST'],
                                                                          port=os.environ['MYSQL_PORT'])

    con = create_engine(constr, encoding='utf-8')
    return con
