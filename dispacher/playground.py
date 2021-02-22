from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import math
import time


PRIMES = [
    112272535095293,
    112582705942171,
    112272535095293,
    115280095190773,
    115797848077099,
    1099726899285419]


def is_prime(n):
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False

    time.sleep(3)

    raise ValueError("stop")

    sqrt_n = int(math.floor(math.sqrt(n)))
    for i in range(3, sqrt_n + 1, 2):
        if n % i == 0:
            return False
    return True


def main():
    with ProcessPoolExecutor() as executor:
        # executor.submit()
        executor.map(is_prime, PRIMES)
       

def simple_main():
    res = map(is_prime, PRIMES)
    print(list(res))


if __name__ == '__main__':
    from timeit import default_timer as timer
    start = timer()
    main()
    # simple_main()
    end = timer()
    print(end - start)
    


# import urllib.request

# URLS = ['http://www.foxnews.com/',
#         'http://www.cnn.com/',
#         'http://europe.wsj.com/',
#         'http://www.bbc.co.uk/',
#         'http://some-made-up-domain.com/']

# # Retrieve a single page and report the URL and contents
# def load_url(url, timeout):
#     with urllib.request.urlopen(url, timeout=timeout) as conn:
#         return conn.read()

# # We can use a with statement to ensure threads are cleaned up promptly
# with ThreadPoolExecutor() as executor:
#     # Start the load operations and mark each future with its URL
#     future_to_url = {executor.submit(load_url, url, 60): url for url in URLS}
#     for future in as_completed(future_to_url):
#         url = future_to_url[future]
#         try:
#             data = future.result()
#         except Exception as exc:
#             print('%r generated an exception: %s' % (url, exc))
#         else:
#             print('%r page is %d bytes' % (url, len(data)))

