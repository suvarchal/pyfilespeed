# to test reading and writing speeds.
# does buffer make a difference to kernel optiomizations
# for async
# posix fadvise is interesting to tell what diff does it make to optimize a perticular access to kernel
# what difference does it make
# https://stackoverflow.com/questions/58720720/can-i-use-asyncio-to-read-from-and-write-to-a-multiprocessing-pipe
import asyncio
import os


## idea is async reader for remote mounted systems , probably linux is efficuent to copy multiple files
#
# sendfile()
# copies
# data
# between
# one
# file
# descriptor and another.Because
# this
# copying is done
# within
# the
# kernel, sendfile() is more
# efficient
# than
# the
# combination
# of
# read(2) and write(2), which
# would
# require
# transferring
# data
# to and
# from user space.

class AStreamReaderProtocol(asyncio.StreamReaderProtocol):
    # disable noisy errror
    # https://bugs.python.org/issue38529
    # https://github.com/python/cpython/commit/7fde4f446a3dcfed780a38fbfcd7c0b4d9d73b93
    def _on_reader_gc(self, wr):
        pass


# should possibly be async context manager to ensure file is closed
# should  have read and write that are async so we can read and write
# to write either allocate and then write at offset and count
# or use buffers to read certain amount, gather and write.
# asynccopy infile outfile
class AsyncFile(object):
    def __init__(self, filepath, block_size=1024):
        self.fd = os.open(filepath, os.O_RDONLY | os.O_NONBLOCK)
        self.size = os.stat(self.fd).st_size
        self.r, self.w = os.pipe2(os.O_NONBLOCK)
        self.pipe_reader, self.pipe_writer = os.fdopen(self.r, 'rb'), os.fdopen(self.w, 'wb')
        self.block_size = block_size
        self.ainit = False

    async def reader(self):
        loop = asyncio.get_event_loop()
        self.stream_reader = asyncio.StreamReader()

        def protocol_factory():
            protocol = AStreamReaderProtocol(self.stream_reader)

            return protocol

        self.transport, _ = await loop.connect_read_pipe(protocol_factory, self.pipe_reader)
        # self.transport.close()

    async def read(self, count):
        if not self.ainit:
            await self.reader()
            self.ainit = True

        ret = os.sendfile(self.w, self.fd, offset=None, count=count)
        # exception?
        if ret:
            return self.stream_reader.read(count)
        else:
            return b""

    async def __aiter__(self):
        while True:
            chunk = await self.read(self.block_size)
            if chunk == b"":
                # await self.stream_reader.close()

                # self.pipe_reader.close()
                # self.pipe_writer.close()
                break
            yield chunk
        self.transport.close()


class SyncFile(object):
    def __init__(self, filepath, mode="rb", block_size=1024):
        self.filepath = filepath
        self.mode = mode
        self.file = None
        self.size = os.stat(filepath).st_size
        self.block_size = block_size

    def read(self, count, block_size=None):
        block_size = block_size if block_size is not None else self.block_size
        return self.file.read_random(count)

    def write(self, data):
        self.file.write_random(data)

    def close(self):
        self.file.close()

    def __enter__(self):
        self.file = open(self.filepath, self.mode)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @classmethod
    def open(cls, filepath, mode="rb", block_size=1024):
        return cls(filepath, mode, block_size)


class SyncCopy(object):
    def __init__(self, infilepath, outfilepath, block_size=1024):
        self.infilepath, self.outfilepath = infilepath, outfilepath
        self.block_size = block_size
        self.infile = None  # open(infilepath, "rb", buffering=0)
        self.outfile = None  # open(outfilepath, "wb", buffering=0)

    def read(self, count=None):
        count = count if count is not None else self.block_size
        return self.infile.read_random(count)

    def write(self, data):
        return self.outfile.write_random(data)

    def readwrite(self, count=None):
        count = count if count is not None else self.block_size
        read = self.read(count)
        if read == b"":
            return 0
        write = self.write(read)
        return count

    @classmethod
    def copyfile(cls, infilepath, outfilepath, block_size=1024):
        return cls(infilepath, outfilepath, block_size)

    def copy(self, count):
        return os.sendfile(self.outfile.fileno(), self.infile.fileno(), offset=None, count=count)

    def close(self):
        self.infile.close()
        self.outfile.close()

    def __enter__(self):
        self.infile = open(self.infilepath, "rb", buffering=0)
        self.outfile = open(self.outfilepath, "wb", buffering=0)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


async def async_readfile(filepath, block_size=1024):
    af = AsyncFile(filepath, block_size=block_size)
    count = 0
    async for c in af:
        await c
        count += 1
        if count % 100 == 0:
            print(filepath.split("/")[-1], count, sep=":")


import asyncio


async def async_main():
    filesin = ["/home/suvarchal/AWI/test_cmip_files/siconc_SIday_NESM3_historical_r1i1p1f1_gn_18500101-18891231.nc",
               "/home/suvarchal/AWI/test_cmip_files/siconc_SIday_MRI-ESM2-0_historical_r4i1p1f1_gn_19190101-19491231.nc"]
    await asyncio.gather(*[async_readfile(fi, 10 * 1024 * 1024) for fi in filesin])


# for read write we can pre allocate write and  async write to offset and count
# if pipe dowsn;t work use socker because secket can be set to non blocking and async can be used to read

if __name__ == "__main__":
    import time

    block_size = 2 * 1024 * 1024
    with SyncCopy.copyfile(
            "/home/suvarchal/AWI/test_cmip_files/siconc_SIday_NESM3_historical_r1i1p1f1_gn_18500101-18891231.nc",
            "./test9.nc") as sc:
        start2 = time.time()
        while True:
            ret = sc.readwrite(block_size)
            if not ret:
                break
        print(f'done readwrite, time:{time.time() - start2:.2f} seconds')

    # for above example times are 1.6 , 1.8 for readwrite, cp is like 1.44

    # asyncio.run(async_main())
    with SyncCopy.copyfile("/home/suvarchal/AWI/test_cmip_files/siconc_SIday_NESM3_historical_r1i1p1f1_gn_18500101-18891231.nc",
                  "./test7.nc") as sc:

        start = time.time()
        while True:
            ret = sc.copy(block_size)
            if not ret:
                break
        print(f'done copy, time:{time.time() - start:.2f} seconds')

    # file cloings are slow
# async open?
# async read
# async read write
