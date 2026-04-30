import math
import random
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple


def laplace_noise(scale: float) -> float:
    """
    Sample Laplace(0, scale) noise using inverse CDF.
    """
    if scale <= 0:
        raise ValueError("scale must be > 0")
    u = random.random() - 0.5  # in (-0.5, 0.5)
    return -scale * math.copysign(1.0, u) * math.log(1 - 2 * abs(u))


@dataclass
class DifferentialPrivacy:
    """
    MVP DP utilities for adding Laplace noise to numeric aggregates.
    """

    sensitivity: float

    def add_laplace_noise(self, value: float, epsilon: float) -> float:
        if epsilon <= 0:
            raise ValueError("epsilon must be > 0")
        scale = self.sensitivity / epsilon
        return value + laplace_noise(scale)

    def dp_noisy_counts(self, true_counts: Dict[str, float], epsilon: float, clamp_min: float = 0.0) -> Dict[str, float]:
        return {
            k: max(clamp_min, self.add_laplace_noise(v, epsilon))
            for k, v in true_counts.items()
        }


def epsilon_sweep_mae(
    true_counts: Dict[str, float],
    dp: DifferentialPrivacy,
    epsilons: Iterable[float],
    num_trials: int = 20,
) -> List[Tuple[float, float]]:
    """
    For each epsilon, run DP multiple times and compute mean absolute error (MAE).
    """
    epsilons = list(epsilons)
    keys = list(true_counts.keys())

    out: List[Tuple[float, float]] = []
    for eps in epsilons:
        abs_errs: List[float] = []
        for _ in range(num_trials):
            noisy = dp.dp_noisy_counts(true_counts, eps)
            for k in keys:
                abs_errs.append(abs(noisy[k] - true_counts[k]))
        out.append((eps, sum(abs_errs) / max(len(abs_errs), 1)))
    return out

