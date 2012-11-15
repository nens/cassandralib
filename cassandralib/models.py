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


class BucketFormat:
    HOURLY = '%Y-%m-%dT%H'
    DAILY = '%Y-%m-%d'
    MONTHLY = '%Y-%m'
    YEARLY = '%Y'


def bucket_format(sensor_id):
    return BucketFormat.YEARLY


def bucket_delta(bucketformat):
    if (bucketformat == BucketFormat.HOURLY):
        return relativedelta( hours = +1 )
    if (bucketformat == BucketFormat.DAILY):
        return relativedelta( days = +1 )
    if (bucketformat == BucketFormat.MONTHLY):
        return relativedelta( months = +1 )
    if (bucketformat == BucketFormat.YEARLY):
        return relativedelta( years = +1 )


def bucket_start(timestamp, bucketformat):
    if (bucketformat == BucketFormat.HOURLY):
        return timestamp.replace(minute=0, second=0, microsecond=0)
    if (bucketformat == BucketFormat.DAILY):
        return timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
    if (bucketformat == BucketFormat.MONTHLY):
        return timestamp.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if (bucketformat == BucketFormat.YEARLY):
        return timestamp.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)


class CassandraDataStore:

    def __init__(self, nodes, keyspace, column_family, queue_size):
        pool = pycassa.ConnectionPool(keyspace=keyspace, server_list=nodes,
            prefill=False)
        self.cf = pycassa.ColumnFamily(pool, column_family)
        self.batch = self.cf.batch(queue_size=queue_size)

    def read(self, sensor_id, start, end, params=[]):
        assert start.tzinfo != None, "Start datetime must be timezone aware"
        assert end.tzinfo != None, "End datetime must be timezone aware"
        assert str(start.tzinfo.utcoffset(start))[1:] == ':00:00' or \
            str(start.tzinfo.utcoffset(start))[1:] == ':30:00', \
            "Start datetime has weird utc offset; use tz.localize"
        assert str(end.tzinfo.utcoffset(end))[1:] == ':00:00' or \
            str(end.tzinfo.utcoffset(end))[1:] == ':30:00', \
            "End datetime has weird utc offset; use tz.localize"
        
        # The bucket format defines how much data is on one Cassandra row.
        format = bucket_format(sensor_id)

        key_format = sensor_id + ":" + format
        stamp = bucket_start(start.astimezone(INTERNAL_TIMEZONE), format)
        delta = bucket_delta(format)

        col_start = start.astimezone(INTERNAL_TIMEZONE).strftime(COLNAME_FORMAT)
        col_end = end.astimezone(INTERNAL_TIMEZONE).strftime(COLNAME_FORMAT)

        datetimes = {}
        data = {}

        # From each bucket within in the specified range, get the columns
        # within the specified range.
        rowkeys = []
        while stamp < end:
            rowkeys.append(stamp.strftime(key_format))
            stamp += delta
        print "Get " + repr(rowkeys) + " with filter " + \
            col_start + " " + col_end
        try:
            result = self.cf.multiget(rowkeys,
                column_start=col_start, column_finish=col_end)
            for rowkey in result:
                for col_name in result[rowkey]:
                    bits = col_name.split(COLNAME_SEPERATOR)
                    if (len(bits) > 1):
                        dt = datetime.strptime(bits[0], COLNAME_FORMAT)
                        key = col_name[len(bits[0]) + 1:]
                        if (key in params) or (len(params) == 0):
                            if not dt in datetimes.keys():
                                datetimes[dt] = {}
                            datetimes[dt][key] = result[rowkey][col_name]
                            if not key in data.keys():
                                data[key] = []
        except NotFoundException:
            pass
        except Exception as ex:
            print repr(ex)
        # Now, reform the data by series.
        for dt in sorted(datetimes):
            for key in data:
                if key in datetimes[dt]:
                    data[key].append(datetimes[dt][key])
                else:
                    data[key].append(None)
        # And create the Pandas DataFrame.
        return pd.DataFrame(data=data, index=sorted(datetimes))

    def write_frame(self, sensor_id, df):
        for timestamp, row in df.iterrows():
            self.write_row(sensor_id, timestamp, row.to_dict())

    def write_row(self, sensor_id, timestamp, row):
        ts_int = timestamp.astimezone(INTERNAL_TIMEZONE)
        key = ts_int.strftime(sensor_id + ':' + bucket_format(sensor_id))
        stamp = ts_int.strftime(COLNAME_FORMAT)
        self.batch.insert(key, dict(
            ("%s%s%s" % (stamp, COLNAME_SEPERATOR, k), str(v))
            for k, v in row.iteritems()
        ))

    def flush(self):
        self.batch.send()
