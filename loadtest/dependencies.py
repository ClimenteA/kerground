import time
from kerground import Kerground

ker = Kerground()

# You can do this anywhere in your app
@ker.register(ker.MODE.PROCESS)
def convert_files(event: list[str]):
    time.sleep(0.5)

