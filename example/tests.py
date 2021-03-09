import unittest
import os, time
from kerground import Kerground

ker = Kerground()


class TestKerground(unittest.TestCase):

    def test_send_task(self):
        id = ker.send('long_task')
        self.assertIsInstance(id, str)

    def test_send_multiple_tasks(self):
        
        tasks_sent = []
        for _ in range(20):
            id = ker.send('long_task')
            tasks_sent.append(id+".pickle")

        files = os.listdir(ker.storage_path)
        saved = list(set.intersection(set(files), set(tasks_sent)))

        self.assertCountEqual(tasks_sent, saved)


    def test_check_statuses(self):

        stats_dict = ker.stats()

        print(stats_dict)

        if not stats_dict['failed']: return

        print("\n\n")
        [print(id, ker.get_response(id)) for id in stats_dict['failed']]
       
        


       




if __name__ == '__main__':
    unittest.main()