# (c) Nelen & Schuurmans.  MIT licensed, see LICENSE.rst.
from __future__ import unicode_literals
from dateutil.relativedelta import relativedelta
from datetime import datetime
from pycassa.cassandra.ttypes import NotFoundException

import pandas as pd
import numpy as np
import pycassa
import pytz


INTERNAL_TIMEZONE = pytz.UTC
COLNAME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
COLNAME_FORMAT_MS = '%Y-%m-%dT%H:%M:%S.%fZ'
COLNAME_SEPERATOR = '_'
MAX_COLUMNS = 2147483647


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
        self.read_consistency_level = pycassa.ConsistencyLevel.ONE
        self.write_consistency_level = pycassa.ConsistencyLevel.QUORUM

    def _get_column_family(self, column_family):
        if column_family not in self._column_families:
            self._column_families[column_family] = \
                pycassa.ColumnFamily(self.pool, column_family, \
                    write_consistency_level=self.write_consistency_level, \
                    read_consistency_level=self.read_consistency_level)
        return self._column_families[column_family]

    def _get_batch(self, column_family):
        if column_family not in self._batches:
            cf = self._get_column_family(column_family)
            self._batches[column_family] = cf.batch(queue_size=self.queue_size)
        return self._batches[column_family]

    def read(self, column_family, sensor_id, start, end, params=[],
             convert_values_to=None):
        if start:
            assert start.tzinfo is not None, \
                "Start datetime must be timezone aware"
            assert str(start.tzinfo.utcoffset(start))[1:] == ':00:00' or \
                str(start.tzinfo.utcoffset(start))[1:] == ':30:00', \
                "Start datetime has weird utc offset; use tz.localize"
        if end:
            assert end.tzinfo is not None, \
                "End datetime must be timezone aware"
            assert str(end.tzinfo.utcoffset(end))[1:] == ':00:00' or \
                str(end.tzinfo.utcoffset(end))[1:] == ':30:00', \
                "End datetime has weird utc offset; use tz.localize"

        if start is None or end is None:
            return pd.DataFrame()

        # The bucket format defines how much data is on one Cassandra row.
        format = bucket_format(sensor_id)

        key_format = sensor_id + ":" + format
        stamp = bucket_start(start.astimezone(INTERNAL_TIMEZONE), format)
        delta = bucket_delta(format)

        # From each bucket within in the specified range, get the columns
        # within the specified range.
        rowkeys = []
        while stamp < end:
            rowkeys.append(stamp.strftime(key_format))
            stamp += delta

        # If no Cassandra rows are in requested date range, return nothing.
        if len(rowkeys) == 0:
            return pd.DataFrame()

        col_start = start.astimezone(INTERNAL_TIMEZONE) \
            .strftime(COLNAME_FORMAT_MS)
        col_end = end.astimezone(INTERNAL_TIMEZONE).strftime(COLNAME_FORMAT_MS)

        data = {}
        keys = []

        try:
            result = self._get_column_family(column_family).multiget(
                rowkeys,
                column_start=col_start,
                column_finish=col_end,
                column_count=MAX_COLUMNS
            )
            for rowkey in result:
                for col_name in result[rowkey]:
                    bits = col_name.split(COLNAME_SEPERATOR)
                    if (len(bits) > 1):
                        try:
                            dt = datetime.strptime(bits[0], COLNAME_FORMAT)
                        except ValueError:
                            dt = datetime.strptime(bits[0], COLNAME_FORMAT_MS)
                        key = col_name[len(bits[0]) + 1:]
                        if not params or key in params:
                            if key not in keys:
                                keys.append(key)
                            if not dt in data.keys():
                                data[dt] = {}
                            data[dt][key] = result[rowkey][col_name]
        except NotFoundException:
            pass

        # Flatten the dataset by key.
        # Missing values are converted to None.
        datetimes = sorted(data.keys())
        data_flat = {key: [] for key in keys}
        for dt in datetimes:
            row = data[dt]
            for key in keys:
                value = row.get(key)
                data_flat[key].append(value)

        # Convert values to an appropriate Numpy array type, if requested.
        # Unknown types are kept in their current (Cassandra) form.
        if convert_values_to is not None and 'value' in data_flat:
            dtype_map = {
                'float': np.float32,
                'integer': np.int32
            }
            dtype = dtype_map.get(convert_values_to)
            if dtype is not None:
                # There's a bug in numpy.genfromtxt causing a list of length 1
                # to produce a 0-dim array. Appending and removing a dummy
                # value resolves this.
                length = len(data_flat['value'])
                data_flat['value'].append('dummy')
                data_flat['value'] = \
                    np.genfromtxt(data_flat['value'], dtype=dtype)[:length]

        # And create the Pandas DataFrame.
        result = pd.DataFrame(data=data_flat, index=datetimes)
        if len(datetimes) > 0:
            result.tz_localize(INTERNAL_TIMEZONE, copy=False)
        return result

    def write_row(self, column_family, sensor_id, timestamp, row):
        ts_int = timestamp.astimezone(INTERNAL_TIMEZONE)
        key = ts_int.strftime(sensor_id + ':' + bucket_format(sensor_id))
        stamp = ts_int.strftime(COLNAME_FORMAT_MS)
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
