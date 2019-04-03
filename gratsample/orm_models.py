# example taken from http://pythoncentral.io/introductory-tutorial-python-sqlalchemy/
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, BigInteger, Index, Float, ForeignKey, TEXT
from sqlalchemy.dialects.mysql import TINYTEXT, MEDIUMTEXT, LONGTEXT, JSON
from sqlalchemy.ext.declarative import declarative_base
import datetime

Base = declarative_base()

class candidates(Base):
    __tablename__ = 'candidates'
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8', 'mysql_collate': 'utf8_general_ci'}
    candidate_id                = Column(Integer, primary_key=True)
    created_at                  = Column(DateTime, index=True, default=datetime.datetime.utcnow)
    lang                        = Column(TINYTEXT)
    user_id                     = Column(Integer, index=True)
    user_name                   = Column(TINYTEXT)
    user_registration           = Column(DateTime)
    user_editcount              = Column(Integer)
    user_experience_level       = Column(TINYTEXT, default=False)
    thanks_sent                 = Column(Integer)
    thanks_received             = Column(Integer)
    has_email                   = Column(Boolean)
    user_thanked                = Column(Boolean, default=False)

class edits(Base):
    __tablename__ = 'edits'
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8', 'mysql_collate': 'utf8_general_ci'}
    edit_id                     = Column(Integer, primary_key=True)
    candidate_id                = Column(Integer, ForeignKey("candidates.candidate_id"))
    lang                        = Column(TINYTEXT)
    rev_id                      = Column(Integer)
    page_name                   = Column(TEXT)
    page_id                     = Column(Integer)
    ores_damaging               = Column(Boolean, default=None)
    ores_goodfaith              = Column(Boolean, default=None)
    de_flagged                  = Column(Boolean, default=None)
    de_flagged_algo_version     = Column(Boolean, default=None)
    edit_deleted                = Column(default=False)
    diffHTML                    = Column(MEDIUMTEXT)
    newRevId                    = Column(Integer)
    newRevDate                  = Column(TINYTEXT)
    newRevUser                  = Column(TINYTEXT)
    newRevComment               = Column(TEXT)
    oldRevId                    = Column(Integer)
    oldRevDate                  = Column(TINYTEXT)
    oldRevUser                  = Column(TINYTEXT)
    oldRevComment               = Column(TEXT)
