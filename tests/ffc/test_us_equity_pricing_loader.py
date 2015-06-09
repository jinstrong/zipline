"""
Tests for zipline.data.ffc.loaders.us_equity_pricing
"""
from unittest import TestCase

import bcolz
import numpy as np
import pandas as pd

from zipline.data.equities import USEquityPricing
from zipline.data.ffc.loaders.us_equity_pricing import (
    BcolzRawPriceLoader
)

import us_equity_pricing_test_data


class UsEquityPricingLoaderTestCase(TestCase):

    @classmethod
    def setUpClass(cls):
        us_equity_pricing_test_data.create_test_bcolz_data()

    def test_load_from_bcolz(self):
        # 1) The equity's trades cover all query dates.
        # 2) The equity's trades are all before the start of the query.
        # 3) The equity's trades start before the query start, but stop
        #    before the query end.
        # 4) The equity's trades start after query start but end before
        #    the query end.
        # 5) The equity's trades start after query start, but trade through or
        #    past the query end
        # 6) The equity's trades are start after query end.
        assets = pd.Int64Index(us_equity_pricing_test_data.EQUITY_INFO.keys())
        columns = [USEquityPricing.close, USEquityPricing.volume]
        query_dates = us_equity_pricing_test_data.TEST_QUERY_RANGE
        table = bcolz.ctable(
            rootdir=us_equity_pricing_test_data.BCOLZ_TEST_DATA_PATH,
            mode='r')
        trading_days = us_equity_pricing_test_data.TEST_TRADING_DAYS
        raw_price_loader = BcolzRawPriceLoader(table, trading_days)
        raw_arrays = raw_price_loader.load_raw_arrays(
            columns,
            assets,
            query_dates)

        close_array = raw_arrays[0]
        # See create_test_bcolz_data for encoding of expected values.
        # Created as the column, so that test is isolated on the individual
        # asset date ranges.
        expected = [
            # Asset 1 should have trade data for all days.
            np.array([14.004, 14.005, 14.006, 14.007, 14.008]),
            # Asset 2 should have no values, all data occurs before query.
            np.array([np.nan, np.nan, np.nan, np.nan, np.nan]),
            # Asset 3 should have the first two days of data.
            np.array([34.004, 34.005, np.nan, np.nan, np.nan]),
            # Asset 4 should have data starting on the second day and ending
            # on the third.
            np.array([np.nan, 44.001, 44.002, np.nan, np.nan]),
            # Asset 4 should have data starting on the second day through the
            # end of the range
            np.array([np.nan, 54.001, 54.002, 54.003, 54.004]),
            # Asset 5 should have no data
            np.array([np.nan, np.nan, np.nan, np.nan, np.nan])
        ]

        for i, expected_col in enumerate(expected):
            np.testing.assert_allclose(expected_col, close_array[:, i])

        import pprint; import nose; nose.tools.set_trace()
        assert True
