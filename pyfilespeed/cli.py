import os
import random
import string

from tqdm import tqdm
from tqdm.utils import CallbackIOWrapper

from .utils import gen_random_bytes

# tqdm has an issue that it starts time at initanciation and all ops in between are counted.

def write_random(dstpath=".", size=100 * 1024 * 1024, block_size=1024 * 1024):
    random_file = os.path.join(dstpath, "tmp_" + "".join(random.choice(string.ascii_lowercase) for _ in range(5)))
    #null = open("/dev/urandom", "rb")
    chunk = gen_random_bytes(block_size) #null.read(block_size) # better so that kernel doesn't cache
    # with tqdm.wrapattr(open(random_file, "wb"), "write", miniters=1, total=size, unit='B', unit_scale=True, unit_divisor=1024,
    #                   desc=f"Write: {random_file}") as fout:
    with open(random_file, "wb") as fout:
        t = tqdm(total=size, miniters=1,
                  unit='B', unit_scale=True, unit_divisor=1024, desc=f"Write: {random_file}")
        fobj = CallbackIOWrapper(t.update, fout, "write")
        count = size
        while True:
                #chunk = null.read(block_size)  # gen_random_bytes(block_size)
                #chunk = gen_random_bytes(block_size)
                fobj.write(chunk)
                count = count - block_size
                if count <= 0:
                    break
    #null.close()
    t.close()
    return random_file


def read_random(src_file, block_size=1024 * 1024):
    size = os.stat(src_file).st_size
    with tqdm.wrapattr(open(src_file, "rb"), "read", miniters=1, total=size, unit='B', unit_scale=True,
                       unit_divisor=1024,
                       desc=f"Read :{src_file}") as fin:
        # with open(src_file, "rb") as fin:
        count = 0
        while True:
            chunk = fin.read(block_size)
            count += len(chunk)
            if chunk == b"":
                break


# st = time.time()
#rf = write_random(".", size=1024 * 1024 * 1024)
# print(f'total time:{time.time() - st:.2f} seconds')

# st = time.time()
# read_random("/home/suvarchal/AWI/test_cmip_files/siconc_SIday_NESM3_historical_r1i1p1f1_gn_18500101-18891231.nc", block_size=2*1024*1024)
#read_random(rf, block_size=2 * 1024 * 1024)
# print(f'total time:{time.time() - st:.2f} seconds')

def runner(path=None):
    import sys

    path = path if path is not None else "." #sys.argv[1] if len(sys.argv)>0 else "."
    n = 4 #int(sys.argv[2]) if len(sys.argv)>1 else 1
    bs = 1024*1024 #int(sys.argv[3]) if len(sys.argv)>2 else 128*1024
    size = 1024*1024*1024 #int(sys.argv[4]) if len(sys.argv)>3 else 1024*1024*1024

    written_files = [write_random(path, size, bs) for _ in range(n)]
    read_files = [read_random(fi) for fi in written_files]
    for fi in written_files:
        os.remove(fi)


# because we know sizes of blocks we can estimate speed and compute avg
# if read has callback then update

if __name__ == "__main__":
    runner()
