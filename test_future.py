import concurrent.futures

def fail():
    raise BaseException("FAIL")

with concurrent.futures.ProcessPoolExecutor(max_workers=2) as e:
    f = e.submit(fail)
    print(f.exception())
