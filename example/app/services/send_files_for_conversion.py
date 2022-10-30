import time
import random
from typing import List
from app.dependencies import UploadFile
from app.repository import Repository
from app.dependencies import ker


@ker.register(ker.MODE.THREAD, max_retries=1)
def _send_files_for_conversion(filepaths: List[str]):
    time.sleep(2)  # some heavy processing here
    Repository.save_files(filepaths)
    if random.choice([0, 1]):
        raise Exception("Some unexpected exception")


def send_files_for_conversion(files: List[UploadFile]):
    # Mimic save files to disk
    filepaths = [file.filename for file in files]
    msgid = ker.enqueue("_send_files_for_conversion", filepaths)
    return msgid
