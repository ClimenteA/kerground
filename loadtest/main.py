import time
from kerground import Kerground

ker = Kerground()


@ker.register(ker.MODE.PROCESS)
def convert_files(event: list[str]):
    time.sleep(0.5)


if __name__ == "__main__":

    use_kerground = True
    tasks = 100

    if not use_kerground:
        print("Started without kerground...")
        startnoker = time.perf_counter()
        for _ in range(tasks):
            convert_files(['filepaths'])
        ellapesednoker = time.perf_counter() - startnoker
        print(f"It took...{ellapesednoker} seconds")

    if use_kerground:
        print("Started with kerground...")
        startker = time.perf_counter()
        for _ in range(tasks):
            ker.enqueue("convert_files", ['filepaths'])
        ker.listen()