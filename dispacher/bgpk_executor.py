import os
from bgpk_worker import BGPKWorker


class BGPKExecutor(BGPKWorker): 

    def __init__(self):
        super().__init__(_skip_dirs_creation=True)

    @classmethod
    def run(cls):
        print("pending tasks:", cls.pending())
        print("running tasks:", cls.running())
        print("finished tasks:", cls.finished())
        print("failed tasks:", cls.failed())
        
        # with ProcessPoolExecutor() as executor:
        #     pass





BGPKExecutor.run()

    