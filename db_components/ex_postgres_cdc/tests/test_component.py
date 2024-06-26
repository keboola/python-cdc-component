'''
Created on 12. 11. 2018

@author: esner
'''
import os
import unittest

import mock
from freezegun import freeze_time

from db_components.ex_postgres_cdc.src.component import PostgresCDCComponent


class TestComponent(unittest.TestCase):

    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {'KBC_DATADIR': './non-existing-dir'})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = PostgresCDCComponent()
            comp.run()


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
