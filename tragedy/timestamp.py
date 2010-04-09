# TODO: Support time zone
# TODO: Fix UNIX timestamp before year 1918
# test with e1=-1670000000  and e2=-1660000000

from datetime import datetime, timedelta
import uuid
import time

UNIX_T0 = datetime(1970, 1, 1)
MAC_T0 = datetime(1904, 1, 1)
WIN64_T0 = datetime(1601, 1, 1)
UUID_T0 = datetime(1582, 10, 15)

def _import(t0, seconds):
    return t0 + timedelta(seconds=seconds)

def _import2(t0, dt):
    seconds, us = divmod(dt // 10, 1000000)
    delta = timedelta(seconds=seconds, microseconds=us)
    return t0 + delta

def _export(t0, t, microseconds):
    delta = t - t0
    seconds = delta.days * 3600 * 24 + delta.seconds
    if microseconds:
        seconds += delta.microseconds / 1e6
    return seconds

def _export2(t0, t):
    delta = t - t0
    seconds = delta.days * 3600 * 24 + delta.seconds
    return (seconds * 1000000 + delta.microseconds) * 10

def importUnix(epoch):
    """
    >>> importUnix(0)
    datetime.datetime(1970, 1, 1, 0, 0)
    >>> importUnix(1154175644)
    datetime.datetime(2006, 7, 29, 12, 20, 44)
    >>> importUnix(1154175644.47)
    datetime.datetime(2006, 7, 29, 12, 20, 44, 470000)
    >>> importUnix(2147483647)
    datetime.datetime(2038, 1, 19, 3, 14, 7)
    """
    return _import(UNIX_T0, epoch)

def importMac(timestamp):
    """
    >>> importMac(0)
    datetime.datetime(1904, 1, 1, 0, 0)
    >>> importMac(2843043290)
    datetime.datetime(1994, 2, 2, 14, 14, 50)
    >>> importMac(2843043290.123456)
    datetime.datetime(1994, 2, 2, 14, 14, 50, 123456)
    """
    return _import(MAC_T0, timestamp)

def importWin64(timestamp):
    """
    >>> importWin64(0)
    datetime.datetime(1601, 1, 1, 0, 0)
    >>> importWin64(127840491566710000)
    datetime.datetime(2006, 2, 10, 12, 45, 56, 671000)
    """
    return _import2(WIN64_T0, timestamp)

def importUUID(timestamp):
    """
    >>> importUUID(0)
    datetime.datetime(1582, 10, 15, 0, 0)
    >>> importUUID(130435676263032360)
    datetime.datetime(1996, 2, 14, 5, 13, 46, 303236)
    """
    return _import2(UUID_T0, timestamp)

def exportUnix(t, microseconds=False):
    """
    >>> t0 = datetime(1970, 1, 1, 0, 0)
    >>> exportUnix(t0)
    0
    >>> t1 = datetime(2006, 7, 29, 12, 20, 44)
    >>> exportUnix(t1)
    1154175644
    >>> t2 = datetime(2006, 7, 29, 12, 20, 44, 470000)
    >>> exportUnix(t2)
    1154175644
    >>> exportUnix(t2, microseconds=True)
    1154175644.47
    >>> t3 = datetime(2038, 1, 19, 3, 14, 7)
    >>> exportUnix(t3)
    2147483647
    """
    return _export(UNIX_T0, t, microseconds)

def exportMac(t, microseconds=False):
    """
    >>> t1 = datetime(1904, 1, 1)
    >>> exportMac(t1)
    0
    >>> t2 = datetime(1994, 2, 2, 14, 14, 50, 123456)
    >>> exportMac(t2)
    2843043290L
    >>> exportMac(t2, microseconds=True)
    2843043290.123456
    """
    return _export(MAC_T0, t, microseconds)

def exportWin64(t):
    """
    >>> t1 = datetime(1601, 1, 1)
    >>> exportWin64(t1)
    0
    >>> t2 = datetime(2006, 2, 10, 12, 45, 56, 671000)
    >>> exportWin64(t2)
    127840491566710000L
    """
    return _export2(WIN64_T0, t)

def exportUUID(timestamp):
    """
    >>> t1 = datetime(1582, 10, 15, 0, 0)
    >>> exportUUID(t1)
    0
    >>> t2 = datetime(1996, 2, 14, 5, 13, 46, 303236)
    >>> exportUUID(t2)
    130435676263032360L
    """
    return _export2(UUID_T0, timestamp)

def fromUUID(uuinp):
    return exportUnix(importUUID( uuinp.time),microseconds=True)

if __name__ == "__main__":
    import doctest
    doctest.testmod()

