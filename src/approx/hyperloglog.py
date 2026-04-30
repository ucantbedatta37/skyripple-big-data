import hashlib
import math
from dataclasses import dataclass


def _hash64(x: str) -> int:
    h = hashlib.blake2b(x.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(h, byteorder="big", signed=False)


@dataclass
class HyperLogLog:
    """
    Minimal HyperLogLog (HLL) implementation for cardinality estimation (MVP).

    This is used so we can compute the required probabilistic statistics locally
    and later validate them against exact baselines.
    """

    p: int = 10  # number of index bits; m = 2^p registers

    def __post_init__(self) -> None:
        if not (4 <= self.p <= 20):
            raise ValueError("p should be in a reasonable range (4..20) for MVP")
        self.m = 1 << self.p
        self._registers = [0] * self.m
        # Bias-correction constant
        if self.m == 16:
            self.alpha_m = 0.673
        elif self.m == 32:
            self.alpha_m = 0.697
        elif self.m == 64:
            self.alpha_m = 0.709
        else:
            self.alpha_m = 0.7213 / (1 + 1.079 / self.m)

    def _rho(self, w: int) -> int:
        # Position of first 1 in (w) from LSB side.
        # We use leading-zero count on the remaining bits by mapping w to 64-p bits.
        if w == 0:
            return 64 - self.p
        lz = (64 - self.p) - w.bit_length()
        return lz + 1

    def add(self, item: str) -> None:
        x = _hash64(item)
        idx = x & (self.m - 1)
        w = x >> self.p
        self._registers[idx] = max(self._registers[idx], self._rho(w))

    def estimate(self) -> float:
        # Raw estimate
        inv_sum = 0.0
        for reg in self._registers:
            inv_sum += 2.0 ** (-reg)
        raw = self.alpha_m * self.m * self.m / inv_sum

        # Small range correction (linear counting)
        v = self._registers.count(0)
        if raw <= (5.0 / 2.0) * self.m and v > 0:
            return self.m * math.log(self.m / v)

        # Large range correction is ignored in MVP (rare for small cardinalities).
        return raw

