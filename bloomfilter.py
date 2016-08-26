# -*- coding:utf-8 -*-
# Created by Vaayne at 2016/08/26 16:01


class BloomFilter(object):
    def __init__(self, connection, bitvector_key, hash_times=10, capacity=10000000):
        # if not (0 < error_rate < 1):
        #     raise ValueError("Error_Rate must be between 0 and 1.")
        if not capacity > 0:
            raise ValueError("Capacity must be > 0")
        self.connection = connection
        self.bitvector_key = bitvector_key
        self.capacity = capacity
        self.hash_times = hash_times

    def __contains__(self, key):
        pipeline = self.connection.pipeline()
        for hashed_offset in self.calculate_offsets(key):
            pipeline.getbit(self.bitvector_key, hashed_offset)
        results = pipeline.execute()
        return all(results)

    @staticmethod
    def FNVHash(key):
        fnv_prime = 0x811C9DC5
        hash = 0
        for i in range(len(key)):
            hash *= fnv_prime
            hash ^= ord(key[i])
        return hash

    @staticmethod
    def APHash(key):
        hash = 0xAAAAAAAA
        for i in range(len(key)):
            if (i & 1) == 0:
                hash ^= ((hash << 7) ^ ord(key[i]) * (hash >> 3))
            else:
                hash ^= (~((hash << 11) + ord(key[i]) ^ (hash >> 5)))
        return hash

    def exist(self, key):
        return self.__contains__(key)

    # add to bloomfilter, if success return True, else return False
    def add(self, key, set_value=1, transaction=False, timeout=None):
        pipeline = self.connection.pipeline(transaction=transaction)
        if self.__contains__(key) and set_value == 1:
            return False
        for hashed_offset in self.calculate_offsets(key):
            pipeline.setbit(self.bitvector_key, hashed_offset, set_value)
        if timeout is not None:
            pipeline.expire(self.bitvector_key, timeout)
        pipeline.execute()
        return True

    # Delete from bloomfilter, if success return True, else return False
    def delete(self, key):
        if self.__contains__(key):
            self.add(key, set_value=0, transaction=True)
            return True
        return False

    def calculate_offsets(self, key):
        hash_1 = self.FNVHash(key)
        hash_2 = self.APHash(key)
        for i in range(self.hash_times):
            yield (hash_1 + i * hash_2) % (self.capacity * 100)
