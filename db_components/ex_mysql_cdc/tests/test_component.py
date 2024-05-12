'''
Created on 12. 11. 2018

@author: esner
'''
import os
import unittest

import mock

from db_components.ex_mysql_cdc.src.component import MySqlCDCComponent


class TestComponent(unittest.TestCase):

    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {'KBC_DATADIR': './non-existing-dir'})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = MySqlCDCComponent()
            comp.run()


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
