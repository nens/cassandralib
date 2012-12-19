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
        return relativedelta(hours=+1)
    if (bucketformat == BucketFormat.DAILY):
        return relativedelta(days=+1)
    if (bucketformat == BucketFormat.MONTHLY):
        return relativedelta(months=+1)
    if (bucketformat == BucketFormat.YEARLY):
        return relativedelta(years=+1)


def bucket_start(timestamp, bucketformat):
    if (bucketformat == BucketFormat.HOURLY):
        return timestamp.replace(
            minute=0, second=0, microsecond=0
        )
    if (bucketformat == BucketFormat.DAILY):
        return timestamp.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    if (bucketformat == BucketFormat.MONTHLY):
        return timestamp.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
    if (bucketformat == BucketFormat.YEARLY):
        return timestamp.replace(
            month=1, day=1, hour=0, minute=0, second=0, microsecond=0
        )


class CassandraDataStore(object):
    _column_families = {}
    _batches = {}

    def __init__(self, nodes, keyspace, queue_size):
        self.pool = pycassa.ConnectionPool(
            keyspace=keyspace, server_list=nodes, prefill=False, max_overflow=0
        )
        self.queue_size = queue_size

    def _get_column_family(self, column_family):
        if column_family not in self._column_families:
            self._column_families[column_family] = \
                pycassa.ColumnFamily(self.pool, column_family)
        return self._column_families[column_family]

    def _get_batch(self, column_family):
        if column_family not in self._batches:
            cf = self._get_column_family(column_family)
            self._batches[column_family] = cf.batch(queue_size=self.queue_size)
        return self._batches[column_family]

    def read(self, column_family, sensor_id, start, end, params=[]):
        assert start.tzinfo is not None, \
            "Start datetime must be timezone aware"
        assert end.tzinfo is not None, \
            "End datetime must be timezone aware"
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

        col_start = start.astimezone(INTERNAL_TIMEZONE) \
            .strftime(COLNAME_FORMAT)
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
            result = self._get_column_family(column_family).multiget(
                rowkeys, column_start=col_start, column_finish=col_end
            )
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
        # Now, reform the data by series.
        for dt in sorted(datetimes):
            for key in data:
                if key in datetimes[dt]:
                    data[key].append(datetimes[dt][key])
                else:
                    data[key].append(None)
        # And create the Pandas DataFrame.
        return pd.DataFrame(data=data, index=sorted(datetimes))

    def write_row(self, column_family, sensor_id, timestamp, row):
        ts_int = timestamp.astimezone(INTERNAL_TIMEZONE)
        key = ts_int.strftime(sensor_id + ':' + bucket_format(sensor_id))
        stamp = ts_int.strftime(COLNAME_FORMAT)
        self._get_batch(column_family).insert(key, dict(
            ("%s%s%s" % (stamp, COLNAME_SEPERATOR, k), str(v))
            for k, v in row.iteritems()
        ))

    def truncate(self, column_family):
        self._get_column_family(column_family).truncate()

    def commit(self, column_family):
        if column_family in self._batches.keys():
            self._batches[column_family].send()
            del self._batches[column_family]
