import hashlib
from dataclasses import dataclass
from typing import List


def _hash64_with_seed(x: str, seed: int) -> int:
    # Seeded, deterministic 64-bit hash using blake2b salt.
    msg = (str(seed) + "|" + x).encode("utf-8")
    h = hashlib.blake2b(msg, digest_size=8).digest()
    return int.from_bytes(h, byteorder="big", signed=False)


@dataclass
class CountMinSketch:
    """
    Minimal Count-Min Sketch (CMS) implementation for frequency estimation.
    """

    width: int = 2048   # w
    depth: int = 5      # d

    def __post_init__(self) -> None:
        if self.width <= 0 or self.depth <= 0:
            raise ValueError("width and depth must be positive")
        self._table: List[List[int]] = [
            [0 for _ in range(self.width)] for _ in range(self.depth)
        ]

    def update(self, key: str, count: int = 1) -> None:
        if count < 0:
            raise ValueError("count must be non-negative")
        for i in range(self.depth):
            h = _hash64_with_seed(key, seed=i)
            j = h % self.width
            self._table[i][j] += count

    def estimate(self, key: str) -> int:
        estimates = []
        for i in range(self.depth):
            h = _hash64_with_seed(key, seed=i)
            j = h % self.width
            estimates.append(self._table[i][j])
        return min(estimates) if estimates else 0

