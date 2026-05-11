"""
Pure logic for somalier-based sex imputation.

Files are read via cpg_utils.Path
so this module can be invoked from inside or outside a Hail Batch job.

Two signals are combined per sequencing group:
- DRAGEN ploidy estimation (passed in per-SG; sourced from MultiQC's DRAGEN_4 section)
- somalier sketch (chrX/chrY allele counts on a fixed sites panel)

The somalier sketch is a binary file produced by `somalier extract` upstream
in dragen_align_pa. Header layout (little-endian):
    version (u8), name_len (u8), name (utf-8, name_len bytes),
    n_auto (u16), n_x (u16), n_y (u16),
followed by (n_auto + n_x + n_y) site records of 12 bytes each:
    nref (u32), nalt (u32), nother (u32).

This module emits a *raw* f_stat (simple ``1 - 2 * x_het_rate`` proxy). Any
population-aware renormalisation (per-ancestry median XX heterozygosity)
lives downstream in ourdna_genomic_atlas, where the ancestry assignments
needed to stratify the median are available.
"""

import math
import struct
from typing import TYPE_CHECKING, Any

from cloudpathlib.exceptions import CloudPathFileNotFoundError
from loguru import logger

from single_sample_qc_popgen.utils import get_dragen_output_path

if TYPE_CHECKING:
    from cpg_flow.targets import SequencingGroup

# Defaults for thresholds used by the imputation rules. Each is overridable
# via the `[impute_sex]` workflow config block (see
# `config/single_sample_qc_popgen.toml`). Module-level constants double as
# defaults for unit tests and ad-hoc invocations.

# Somalier's chrY panel ships ~17 sites (the chrX panel is ~365). On a
# 100+ sample CPG cohort, empirical class distributions on the 17-site
# chrY panel:
#   - True XY:           y_calls >= 15 (essentially full panel)
#   - Normal female:     y_calls in 0-2 (mapping noise / chrX-Y homology)
#   - True X0 / Turner:  y_calls ~ 0
#
# `y_calls > Y_CALLS_LOY_MIN` on a DRAGEN X0 call -> loss-of-Y -> XY.
# Below this gap is treated as "unusual" and left at the upstream DRAGEN call.
Y_CALLS_LOY_MIN = 5

# f_stat ~ 1 looks XY (homozygous chrX); f_stat ~ 0 looks XX (heterozygous).
# Empirical: a 103-sample CPG cohort showed clean bimodal separation around
# 0.5, so a single midpoint is the production default. We flag a sample as
# ambiguous when DRAGEN's call lands on the wrong side of the midpoint:
#   DRAGEN XX + f_stat > F_STAT_XX_MAX -> ambiguous
#   DRAGEN XY + f_stat < F_STAT_XY_MIN  -> ambiguous
# Asymmetric cutoffs (e.g. 0.7 / 0.3) are valid configurations for cohorts
# whose distribution warrants a buffer zone around the midpoint.
F_STAT_XX_MAX = 0.5
F_STAT_XY_MIN = 0.5


def parse_somalier_sketch(data: bytes) -> dict[str, int]:
    """Parse somalier .somalier sketch and count chrX/chrY genotype calls.

    A site is called when nref+nalt > 0; nother is read but ignored.
    Returns x_hom_ref / x_het / x_hom_alt counts, total chrX sites (x_n),
    chrY sites with any reads (y_calls), and total chrY sites (y_n).
    """
    name_len = data[1]
    offset = 2 + name_len
    n_auto, n_x, n_y = struct.unpack_from('<HHH', data, offset)
    offset += 6

    x_offset = offset + n_auto * 12
    y_offset = x_offset + n_x * 12

    x_hom_ref = 0
    x_het = 0
    x_hom_alt = 0
    for i in range(n_x):
        nref, nalt, _ = struct.unpack_from('<III', data, x_offset + i * 12)
        if nref > 0 and nalt == 0:
            x_hom_ref += 1
        elif nref > 0 and nalt > 0:
            x_het += 1
        elif nref == 0 and nalt > 0:
            x_hom_alt += 1

    y_calls = 0
    for i in range(n_y):
        nref, nalt, _ = struct.unpack_from('<III', data, y_offset + i * 12)
        if nref > 0 or nalt > 0:
            y_calls += 1

    return {
        'x_hom_ref': x_hom_ref,
        'x_het': x_het,
        'x_hom_alt': x_hom_alt,
        'x_n': n_x,
        'y_calls': y_calls,
        'y_n': n_y,
    }


def compute_f_stat(x_het: int, x_hom_ref: int, x_hom_alt: int) -> tuple[float, float]:
    """Return (x_het_rate, f_stat_raw).

    f_stat_raw = 1 - 2 * x_het_rate. Both fields are NaN when no chrX sites
    are called.
    """
    n_called = x_hom_ref + x_het + x_hom_alt
    if n_called == 0:
        return float('nan'), float('nan')
    x_het_rate = x_het / n_called
    f_stat_raw = 1 - 2 * x_het_rate
    return x_het_rate, f_stat_raw


def karyotype_from_signals(
    initial_karyotype: str | None,
    f_stat: float,
    y_calls: int,
    *,
    loy_min: int = Y_CALLS_LOY_MIN,
    xx_max: float = F_STAT_XX_MAX,
    xy_min: float = F_STAT_XY_MIN,
) -> str | None:
    """Derive a corrected sex_karyotype from DRAGEN ploidy + somalier signals.

    Rules (thresholds are kwargs with module-level defaults):
      - DRAGEN X0 with chrY signal (y_calls > loy_min) → XY  (loss-of-Y)
      - DRAGEN X0 otherwise                              → X0  (no LoY rescue)
      - DRAGEN XX but f_stat > xx_max       → ambiguous
      - DRAGEN XY but f_stat < xy_min        → ambiguous
      - Otherwise pass through (XX, XY, XXY, …).
    """
    if initial_karyotype is None:
        return None
    if initial_karyotype == 'X0':
        if y_calls > loy_min:
            return 'XY'
        return 'X0'
    if initial_karyotype == 'XX' and not math.isnan(f_stat) and f_stat > xx_max:
        return 'ambiguous'
    if initial_karyotype == 'XY' and not math.isnan(f_stat) and f_stat < xy_min:
        return 'ambiguous'
    return initial_karyotype


def impute_sex_for_cohort(
    cohort_sgs: 'list[SequencingGroup]',
    ploidy_by_sg: dict[str, str | None],
    *,
    loy_min: int = Y_CALLS_LOY_MIN,
    xx_max: float = F_STAT_XX_MAX,
    xy_min: float = F_STAT_XY_MIN,
) -> dict[str, dict[str, Any]]:
    """Read somalier sketches for each SG and combine with the supplied
    DRAGEN ploidy mapping to compute per-sample sex imputation metrics.

    Threshold kwargs default to the module-level constants and are exposed
    so `QCChecker` (or test code) can supply config-driven overrides
    without the module needing to call `config_retrieve` itself.

    Returns a dict keyed by sg.id; each value contains:
        corrected_sex_karyotype: str | None
        f_stat_raw:              float
        x_het_rate:              float
        n_called_x:              int
        y_calls:                 int
        y_n:                     int

    Sequencing groups missing the somalier sketch are skipped with a warning.
    A missing entry in ``ploidy_by_sg`` (or a None value) is tolerated:
    f_stat_raw and y_calls are still computed, and corrected_sex_karyotype
    falls through to None.
    """
    result: dict[str, dict[str, Any]] = {}
    for sg in cohort_sgs:
        somalier_path = get_dragen_output_path(f'somalier/{sg.id}.somalier')
        try:
            sketch_bytes = somalier_path.read_bytes()
        except (FileNotFoundError, CloudPathFileNotFoundError) as e:
            logger.warning(f'Skipping sex imputation for {sg.id}: {e}')
            continue

        sketch = parse_somalier_sketch(sketch_bytes)
        x_het_rate, f_stat_raw = compute_f_stat(
            sketch['x_het'], sketch['x_hom_ref'], sketch['x_hom_alt'],
        )
        n_called_x = sketch['x_hom_ref'] + sketch['x_het'] + sketch['x_hom_alt']
        corrected = karyotype_from_signals(
            ploidy_by_sg.get(sg.id), f_stat_raw, sketch['y_calls'],
            loy_min=loy_min,
            xx_max=xx_max,
            xy_min=xy_min,
        )
        result[sg.id] = {
            'corrected_sex_karyotype': corrected,
            'f_stat_raw': f_stat_raw,
            'x_het_rate': x_het_rate,
            'n_called_x': n_called_x,
            'y_calls': sketch['y_calls'],
            'y_n': sketch['y_n'],
        }
    return result
