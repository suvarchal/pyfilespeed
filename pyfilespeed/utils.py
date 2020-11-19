import random


def gen_random_bytes(count: int) -> bytearray:
    # return os.urandom(count)
    return bytearray(random.getrandbits(8) for _ in range(count))


class RandomByteFile(object):
    @staticmethod
    def read(count):
        return gen_random_bytes(count)

    def __repr__(self):
        return "RandomByteFile"


class ZeroByteFile(object):
    @staticmethod
    def read(count):
        return bytearray(count)

    def __repr__(self):
        return "ZeroByteFile"


# when file is used /dev/urandom we can make async style random gen
# open file in non blocking mode, read

if __name__ == "__main__":
    rf = RandomByteFile()
    assert len(rf.read(10)) == 10, 'cannot assert random bytes'
    zf = ZeroByteFile()
    assert len(zf.read(10)) == 10 and zf.read(10) == zf.read(10), 'cannot assert zero bytes'
