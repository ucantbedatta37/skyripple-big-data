import hashlib
from dataclasses import dataclass
from typing import Iterable


@dataclass
class BloomFilter:
    """
    Simple Bloom filter implementation (MVP).

    We keep it in-memory on the Spark driver inside `foreachBatch`, since the goal
    for the 4-day solo build is an end-to-end working pipeline + rubric evidence.
    """

    m_bits: int
    k_hashes: int

    def __post_init__(self) -> None:
        if self.m_bits <= 0:
            raise ValueError("m_bits must be > 0")
        if self.k_hashes <= 0:
            raise ValueError("k_hashes must be > 0")
        
        # Pre-allocate a mutable bytearray (1 byte = 8 bits). 
        # For 100M bits, this takes ~12.5MB.
        num_bytes = (self.m_bits + 7) // 8
        self._bitarray = bytearray(num_bytes)

    def _hash_positions(self, item: str) -> Iterable[int]:
        for i in range(self.k_hashes):
            h = hashlib.blake2b(
                (str(i) + "|" + item).encode("utf-8"),
                digest_size=8,
            ).digest()
            pos = int.from_bytes(h, byteorder="big") % self.m_bits
            yield pos

    def add(self, item: str) -> None:
        for pos in self._hash_positions(item):
            byte_idx = pos // 8
            bit_idx = pos % 8
            self._bitarray[byte_idx] |= (1 << bit_idx)

    def might_contain(self, item: str) -> bool:
        for pos in self._hash_positions(item):
            byte_idx = pos // 8
            bit_idx = pos % 8
            if not (self._bitarray[byte_idx] & (1 << bit_idx)):
                return False
        return True

