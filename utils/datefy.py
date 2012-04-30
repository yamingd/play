# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

epoch = datetime(1970, 1, 1)
def epoch_seconds(date):
    """Returns the number of seconds from the epoch to date. Should match
    the number returned by the equivalent function in postgres."""
    td = date - epoch
    return td.days * 86400 + td.seconds + (float(td.microseconds) / 1000000)

def tomorrow():
    today = datetime.now()
    yester = today + timedelta(days=1)
    return yester

site_epoch = datetime(2011, 5, 1)
def mksetime(d=datetime.now()):
    td = d-site_epoch
    return td.days * 86400 + td.seconds + (float(td.microseconds) / 1000000)

def now():
    return datetime.now()

def now_epoch():
    return int(time.mktime(datetime.now().timetuple()))

def today():
    dt = datetime.now()
    return datetime(dt.year,dt.month,dt.day)

def today_str():
    return format(today())

def yesterday():
    dt = datetime.now()
    dt = dt - timedelta(days=1)
    return datetime(dt.year,dt.month,dt.day)

def time_as_epoch(value):
    if isinstance(value, datetime):
        return int(time.mktime(value.timetuple()))
    return None

def epoch_as_time(value):
    return datetime.fromtimestamp(float(value))

def format(date, pattern='%Y%m%d'):
    return date.strftime(pattern)