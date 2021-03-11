import unittest
import os, time
from kerground import Kerground

ker = Kerground()


class TestKerground(unittest.TestCase):

    # def test_send_task(self):
    #     id = ker.send('task_no_params')
    #     self.assertIsInstance(id, str)

    def test_send_task_with_args(self):
        id = ker.send('task_with_params', 'param1', 'param2')
        self.assertIsInstance(id, str)

    def test_send_task_with_args_and_timeout(self):
        id = ker.send('task_with_params', 'param1', 'param2', timeout=1)
        self.assertIsInstance(id, str)

    def test_send_multiple_tasks(self):
        
        tasks_sent = []
        for _ in range(5):
            id = ker.send('long_task')
            tasks_sent.append(id+".pickle")

        files = os.listdir(ker.storage_path)
        saved = list(set.intersection(set(files), set(tasks_sent)))

        self.assertCountEqual(tasks_sent, saved)

    def test_get_response(self):
        id = ker.send('task_one_param', "must be returned")
        while ker.status(id) in ['pending', 'running']:
            time.sleep(1)
        res = ker.get_response(id)
        self.assertIsInstance(id, str)
        self.assertEqual(res, "must be returned")


    def test_check_statuses(self):

        stats_dict = ker.stats()

        print(stats_dict)

        if not stats_dict['failed']: return

        print("\n\n")
        [print(id, ker.get_response(id)) for id in stats_dict['failed']]
       
        


       




if __name__ == '__main__':
    unittest.main()