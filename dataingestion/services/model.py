#!/usr/bin/env python
#
# Copyright (c) 2013 Liu, Yonggang <myidpt@gmail.com>, University of Florida
#
# This software may be used and distributed according to the terms of the
# MIT license: http://www.opensource.org/licenses/mit-license.php
"""
This module implements the data model for the service.
"""
from sqlalchemy import (create_engine, Column, Integer, String, DateTime,
                        Boolean, types, distinct)
from sqlalchemy.orm import scoped_session, sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import ForeignKey
from sqlalchemy.sql.expression import desc
import logging, hashlib, argparse, os, time, struct, re, json
from datetime import datetime
import types as pytypes

THRESHOLD_TIME = 2 # sec

Base = declarative_base()

session = None
logger = None

def init(APP_AUTHOR):
  global logger
  logger = logging.getLogger( APP_AUTHOR + '.model')

def setup(db_file):
  """
  Set up the database.
  """
  global session

  db_conn = "sqlite:///%s" % db_file
  logger.info("DB Connection: %s" % db_conn)
  engine = create_engine(db_conn, connect_args={'check_same_thread':False})
  engine.Echo = True
  Base.metadata.create_all(engine)

  Session = scoped_session(sessionmaker(bind=engine))
  session = Session()
  print "DB Connection: %s" % db_conn

def close():
  global session
  if session:
    session.close()
    session = None
