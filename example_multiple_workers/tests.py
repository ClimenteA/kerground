import unittest
import os, time
from kerground import Kerground


pri_ker = Kerground(workers_path="../main_worker")
sec_ker = Kerground(workers_path="../secondary_worker")



class TestKerground(unittest.TestCase):

    def test_send_task(self):
        id1 = pri_ker.send('task_no_params')
        id2 = sec_ker.send('task_no_params')
        self.assertIsInstance(id1, str)
        self.assertIsInstance(id2, str)

    def test_send_task_with_args(self):
        id1 = pri_ker.send('task_with_params', 'param1', 'param2')
        id2 = sec_ker.send('task_with_params', 'param1', 'param2')
        self.assertIsInstance(id1, str)
        self.assertIsInstance(id2, str)

    def test_send_task_with_args_and_timeout(self):
        id1 = pri_ker.send('task_with_params', 'param1', 'param2', timeout=1)
        id2 = sec_ker.send('task_with_params', 'param1', 'param2', timeout=1)
        self.assertIsInstance(id1, str)
        self.assertIsInstance(id2, str)

    def test_send_multiple_tasks(self):
        
        tasks_sent_w1 = []
        tasks_sent_w2 = []
        for _ in range(5):
            id1 = pri_ker.send('long_task')
            id2 = sec_ker.send('long_task')
            tasks_sent_w1.append(id1+".pickle")
            tasks_sent_w2.append(id2+".pickle")

        files_w1 = os.listdir(pri_ker.storage_path)
        saved_w1 = list(set.intersection(set(files_w1), set(tasks_sent_w1)))

        files_w2 = os.listdir(sec_ker.storage_path)
        saved_w2 = list(set.intersection(set(files_w2), set(tasks_sent_w2)))
        
        self.assertCountEqual(tasks_sent_w1, saved_w1)
        self.assertCountEqual(tasks_sent_w2, saved_w2)

    def test_get_response(self):
        id = pri_ker.send('task_one_param', "must be returned", purge=False)
        while pri_ker.status(id) in ['pending', 'running']:
            time.sleep(1)
        res = pri_ker.get_response(id)
        self.assertIsInstance(id, str)
        self.assertEqual(res, "must be returned")

        id = sec_ker.send('task_one_param', "must be returned", purge=False)
        while sec_ker.status(id) in ['pending', 'running']:
            time.sleep(1)
        res = sec_ker.get_response(id)
        self.assertIsInstance(id, str)
        self.assertEqual(res, "must be returned")

    def test_check_statuses(self):

        stats_dict = pri_ker.stats()
        stats_dict = sec_ker.stats()

        print(stats_dict)

        if not stats_dict['failed']: return

        print("\n\n")
        [print(id, pri_ker.get_response(id)) for id in stats_dict['failed']]
        [print(id, sec_ker.get_response(id)) for id in stats_dict['failed']]
       



if __name__ == '__main__':
    unittest.main()