"""
Create data for us_equitiy_pricing tests.
"""
from collections import OrderedDict
import os
import shutil

import bcolz
import numpy as np
import pandas as pd

TEST_DATA_DIR = os.path.join(
    os.path.dirname(__file__),
    'test_data')

BCOLZ_TEST_DATA_PATH = os.path.join(
    TEST_DATA_DIR,
    'equity_test_daily_bars.bcolz')

# Custom trading calendar for us equity test.
TEST_TRADING_DAYS = pd.date_range('2015-05-31', '2015-06-10').tz_localize(
    'UTC')

# Query is smaller than the entire trading days, so that equities that go
# beyond the range are tested..
TEST_QUERY_RANGE = pd.date_range('2015-06-04', '2015-06-08')

# The keys are the asset id.
EQUITY_INFO = OrderedDict((
    # 1) This equity's data covers all dates in range.
    (1, {
        'start_date': '2015-06-01',
        'end_date': '2015-06-10',
    }),
    # 2) The equity's trades are all before the start of the query.
    (2, {
        'start_date': '2015-06-01',
        'end_date': '2015-06-03',
    }),
    # 3) The equity's trades start before the query start, but stop
    #    before the query end.
    (3, {
        'start_date': '2015-06-01',
        'end_date': '2015-06-05',
    }),
    # 4) The equity's trades start after query start and ends before
    #    the query end.
    (4, {
        'start_date': '2015-06-05',
        'end_date': '2015-06-06',
    }),
    # 5) The equity's trades start after query start, but trade through or
    #    past the query end
    (5, {
        'start_date': '2015-06-05',
        'end_date': '2015-06-10',
    }),
    # 6) The equity's trades start and end after query end.
    (6, {
        'start_date': '2015-06-09',
        'end_date': '2015-06-10',
    }),
))

# price type identifiers
PT_OPEN, PT_HIGH, PT_LOW, PT_CLOSE, PT_VOLUME = range(1000, 6000, 1000)


def create_test_bcolz_data():
    sid_col = []
    days = []
    opens = []
    highs = []
    lows = []
    closes = []
    volumes = []

    start_pos = {}
    start_day_offset = {}
    end_day_offset = {}

    for asset, info in EQUITY_INFO.iteritems():
        asset_day_range = pd.date_range(info['start_date'],
                                        info['end_date'])
        asset_len = len(asset_day_range)
        start_pos[asset] = len(sid_col)
        sid_col.extend([asset] * asset_len)

        start_day_offset[asset] = TEST_TRADING_DAYS.searchsorted(
            asset_day_range[0])
        end_day_offset[asset] = TEST_TRADING_DAYS.searchsorted(
            asset_day_range[-1])

        for day in asset_day_range:
            days.append(int(day.strftime("%s")))
        # Prices are 1000 times the equity float, except for volume which is
        # the integer of the float.
        #
        # Create synthetic prices that code information about the price.
        # The  10000 place is the asset id
        # The   1000 place is the price type, i.e. OHLCV
        # The    100 place is the row position of the assets date range
        #            starting at 1 for the first day
        asset_place = int(asset * 10000)
        # Create the row identifier place
        for i in range(asset_len):
            row_id = i + 1
            opens.append(asset_place + PT_OPEN + row_id)
            highs.append(asset_place + PT_HIGH + row_id)
            lows.append(asset_place + PT_LOW + row_id)
            volumes.append(asset_place + PT_VOLUME + row_id)
            closes.append(asset_place + PT_CLOSE + row_id)

    if not os.path.isdir(TEST_DATA_DIR):
        os.mkdir(TEST_DATA_DIR)
    # Clear out existing test data if it exists.
    if os.path.isdir(BCOLZ_TEST_DATA_PATH):
        shutil.rmtree(BCOLZ_TEST_DATA_PATH)

    table = bcolz.ctable(
        names=[
            'open',
            'high',
            'low',
            'close',
            'volume',
            'day',
            'sid'],
        columns=[
            np.array(opens).astype(np.uint32),
            np.array(highs).astype(np.uint32),
            np.array(lows).astype(np.uint32),
            np.array(closes).astype(np.uint32),
            np.array(volumes).astype(np.uint32),
            np.array(days).astype(np.uint32),
            np.array(sid_col).astype(np.uint32),
        ],
        rootdir=BCOLZ_TEST_DATA_PATH,
    )
    table.attrs['start_pos'] = {str(k): v for k, v
                                in start_pos.iteritems()}
    table.attrs['start_day_offset'] = {str(k): v for k, v
                                       in start_day_offset.iteritems()}
    table.attrs['end_day_offset'] = {str(k): v for k, v
                                     in end_day_offset.iteritems()}
    table.flush()


if __name__ == "__main__":
    create_test_bcolz_data()
