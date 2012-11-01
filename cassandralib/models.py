# (c) Nelen & Schuurmans.  MIT licensed, see LICENSE.rst.
from __future__ import unicode_literals
from dateutil.relativedelta import relativedelta
from datetime import datetime
from pycassa.cassandra.ttypes import NotFoundException 
import pandas as pd
import pycassa
import pytz


INTERNAL_TIMEZONE = pytz.UTC
COLNAME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
COLNAME_SEPERATOR = '_'


class BucketSize:
    HOURLY = 4
    DAILY = 3
    MONTHLY = 2
    YEARLY = 1


def bucket_size(sensor_id):
    return BucketSize.YEARLY


def bucket_format(bucketsize):
    if (bucketsize == BucketSize.HOURLY):
        return '%Y-%m-%dT%H'
    if (bucketsize == BucketSize.DAILY):
        return '%Y-%m-%d'
    if (bucketsize == BucketSize.MONTHLY):
        return '%Y-%m'
    if (bucketsize == BucketSize.YEARLY):
        return '%Y'


def bucket_delta(bucketsize):
    if (bucketsize == BucketSize.HOURLY):
        return relativedelta( hours = +1 )
    if (bucketsize == BucketSize.DAILY):
        return relativedelta( days = +1 )
    if (bucketsize == BucketSize.MONTHLY):
        return relativedelta( months = +1 )
    if (bucketsize == BucketSize.YEARLY):
        return relativedelta( years = +1 )


def bucket_start(timestamp, bucketsize):
    if (bucketsize == BucketSize.HOURLY):
        return timestamp.replace(minute=0, second=0, microsecond=0)
    if (bucketsize == BucketSize.DAILY):
        return timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
    if (bucketsize == BucketSize.MONTHLY):
        return timestamp.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if (bucketsize == BucketSize.YEARLY):
        return timestamp.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)


class CassandraDataStore:

    def __init__(self, nodes, keyspace, column_family, queue_size):
        pool = pycassa.ConnectionPool(keyspace=keyspace, server_list=nodes,
            prefill=False)
        self.cf = pycassa.ColumnFamily(pool, column_family)
        self.qs = queue_size

    def read(self, sensor_id, start, end, params=[]):
        assert start.tzinfo != None, "Start datetime must be timezone aware"
        assert end.tzinfo != None, "End datetime must be timezone aware"
        assert str(start.tzinfo.utcoffset(start))[1:] == ':00:00' or \
            str(start.tzinfo.utcoffset(start))[1:] == ':30:00', \
            "Start datetime has weird utc offset; use tz.localize"
        assert str(end.tzinfo.utcoffset(end))[1:] == ':00:00' or \
            str(end.tzinfo.utcoffset(end))[1:] == ':30:00', \
            "End datetime has weird utc offset; use tz.localize"
        
        # The bucket size defines how much data is on one Cassandra row.
        bucket = bucket_size(sensor_id)

        key_format = sensor_id + ":" + bucket_format(bucket)
        stamp = bucket_start(start.astimezone(INTERNAL_TIMEZONE), bucket)
        delta = bucket_delta(bucket)

        col_start = start.astimezone(INTERNAL_TIMEZONE).strftime(COLNAME_FORMAT)
        col_end = end.astimezone(INTERNAL_TIMEZONE).strftime(COLNAME_FORMAT)

        datetimes = {}
        data = {}

        # From each bucket within in the specified range, get the columns
        # within the specified range.
        while stamp < end:
            print "Get " + stamp.strftime(key_format) + " with filter " + \
                col_start + " " + col_end
            try:
                chunk = self.cf.get(stamp.strftime(key_format),
                    column_start=col_start, column_finish=col_end)
                for col_name in chunk:
                    bits = col_name.split(COLNAME_SEPERATOR)
                    if (len(bits) > 1):
                        dt = bits[0]
                        key = col_name[len(dt) + 1:]
                        if (key in params) or (len(params) == 0):
                            if not dt in datetimes.keys():
                                datetimes[dt] = {}
                            datetimes[dt][key] = chunk[col_name]
                            if not key in data.keys():
                                data[key] = []
            except NotFoundException:
                pass
            stamp += delta
        # Now, reform the data by series.
        for dt in sorted(datetimes):
            for key in data:
                if key in datetimes[dt]:
                    data[key].append(datetimes[dt][key])
                else:
                    data[key].append(None)
        # And create the Pandas DataFrame.
        return pd.DataFrame(data=data, index=sorted(datetimes))

    def write(self, sensor_id, df):
        bucket = bucket_size(sensor_id)
        key_format = bucket_format(bucket)
        with self.cf.batch(queue_size=self.qs) as b:
            for timestamp, row in df.iterrows():
                ts_int = timestamp.astimezone(INTERNAL_TIMEZONE)
                b.insert(
                    ts_int.strftime(sensor_id + ':' + key_format),
                    dict(("%s%s%s" % (ts_int.strftime(COLNAME_FORMAT),
                        COLNAME_SEPERATOR, k), str(v))
                        for k, v in row.to_dict().iteritems())
                )
