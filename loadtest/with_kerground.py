import time
from dependencies import ker

# Here we use kerground to process in background

if __name__ == "__main__":

    tasks = 100 

    print("Started with kerground...")
    startker = time.perf_counter()
    for _ in range(tasks):
        ker.enqueue("convert_files", ['filepaths'])
    ker.listen()