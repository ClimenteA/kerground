import time
from dependencies import convert_files

# Here we just call the processing function

if __name__ == "__main__":
    tasks = 100

    print("Started without kerground...")
    startnoker = time.perf_counter()
    for _ in range(tasks):
        convert_files(['filepaths'])
    ellapesednoker = time.perf_counter() - startnoker
    print(f"It took...{ellapesednoker} seconds")
