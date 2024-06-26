'''
Created on 12. 11. 2018

@author: esner
'''
import json
import os
import tempfile
import time
import unittest
import uuid

from keboola.component import CommonInterface

from db_components.db_common import artefacts


class TestComponent(unittest.TestCase):

    def setUp(self):
        self.temp_data_dir = tempfile.mkdtemp()
        with open(os.path.join(self.temp_data_dir, 'config.json'), 'w+') as f:
            json.dump({}, f)
        os.environ['KBC_COMPONENTID'] = 'test-component'
        os.environ['KBC_STACKID'] = 'connection.keboola.com'
        os.environ['KBC_CONFIGID'] = '123'
        os.environ['KBC_CONFIGROWID'] = '456'
        os.environ['KBC_BRANCHID'] = '789'
        os.environ['KBC_PROJECTID'] = '10'
        os.environ['KBC_TOKEN'] = os.environ['KBC_TOKEN']
        self.ci = CommonInterface(data_folder_path=self.temp_data_dir)

    def test_get_artefacts(self):
        test_file = os.path.join(self.temp_data_dir, 'test_data.json')
        with open(test_file, 'w+') as f:
            json.dump({"test": "data"}, f)
        # store artefact
        result_id = artefacts.store_artefact(test_file, self.ci)
        print(f"Result id: {result_id}")

        random_id = uuid.uuid4()
        with open(test_file, 'w+') as f:
            json.dump({"test": f"{random_id}"}, f)
        result_id = artefacts.store_artefact(test_file, self.ci)
        print(f"Result id: {result_id}")
        # get artefact
        expected_tags = [f'test-component-simulated-artefact',
                         f'10-project_id',
                         f'123-config_id',
                         f'456-config_row_id',
                         f'789-branch_id']

        # sleep for 5 seconds to give the artefact time to be stored
        time.sleep(5)

        file_path, tags, result_file_id = artefacts.get_artefact('test_data.json', self.ci)
        self.assertEqual(result_file_id, result_id)
        with open(file_path) as f:
            result = json.load(f)
        # content equals
        self.assertEqual(result['test'], f"{random_id}")
        # tags equal
        self.assertListEqual(tags, expected_tags)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
