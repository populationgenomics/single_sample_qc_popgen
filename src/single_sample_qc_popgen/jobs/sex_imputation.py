"""
Pure logic for somalier-based sex imputation.

Files are read via cpg_utils.Path
so this module can be invoked from inside or outside a Hail Batch job.

Two signals are combined per sequencing group:
- DRAGEN ploidy estimation (from ploidy_estimation_metrics.csv)
- somalier sketch (chrX/chrY allele counts on a fixed sites panel)

The somalier sketch is a binary file produced by `somalier extract` upstream
in dragen_align_pa. Header layout (little-endian):
    version (u8), name_len (u8), name (utf-8, name_len bytes),
    n_auto (u16), n_x (u16), n_y (u16),
followed by (n_auto + n_x + n_y) site records of 12 bytes each:
    nref (u32), nalt (u32), nother (u32).
"""

import statistics
import struct
from typing import TYPE_CHECKING, Any

from cloudpathlib.exceptions import CloudPathFileNotFoundError
from loguru import logger

from single_sample_qc_popgen.utils import get_output_path

if TYPE_CHECKING:
    from cpg_flow.targets import SequencingGroup

MEDIAN_CORRECT_MIN_XX = 10


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


def parse_dragen_ploidy(data: bytes) -> dict[str, Any]:
    """Parse a DRAGEN ploidy_estimation_metrics.csv blob.

    Each line is `PLOIDY ESTIMATION,,<metric>,<value>`. Returns the three
    fields needed downstream; missing fields come back as None.
    """
    result: dict[str, Any] = {
        'ploidy_estimation': None,
        'norm_x_coverage': None,
        'norm_y_coverage': None,
    }
    for raw_line in data.decode('utf-8').splitlines():
        parts = [p.strip() for p in raw_line.split(',')]
        if len(parts) < 4:
            continue
        metric = parts[2]
        value = parts[3]
        if metric == 'Ploidy estimation':
            result['ploidy_estimation'] = value or None
        elif metric == 'X median / Autosomal median' and value:
            result['norm_x_coverage'] = float(value)
        elif metric == 'Y median / Autosomal median' and value:
            result['norm_y_coverage'] = float(value)
    return result


def compute_f_stat(
    x_het: int,
    x_hom_ref: int,
    x_hom_alt: int,
    xx_median_het_rate: float | None = None,
) -> tuple[float, float]:
    """Return (x_het_rate, f_stat).

    Simple proxy: f_stat = 1 - 2*x_het_rate.
    Median-corrected: f_stat = 1 - x_het_rate / xx_median_het_rate.
    Both fields are NaN when no chrX sites are called.
    """
    n_called = x_hom_ref + x_het + x_hom_alt
    if n_called == 0:
        return float('nan'), float('nan')
    x_het_rate = x_het / n_called
    if xx_median_het_rate is not None and xx_median_het_rate > 0:
        f_stat = 1 - (x_het_rate / xx_median_het_rate)
    else:
        f_stat = 1 - 2 * x_het_rate
    return x_het_rate, f_stat


def karyotype_from_signals(
    initial_karyotype: str | None,
    f_stat: float,
    y_calls: int,
) -> str | None:
    """Derive a corrected sex_karyotype from DRAGEN ploidy + somalier signals.

    Rules:
      - X0 with chrY signal (y_calls > 5)        → XY  (loss-of-Y)
      - X0 without chrY signal (y_calls <= 1)    → X0  (true Turner-like)
      - DRAGEN XX but f_stat > 0.7               → ambiguous
      - DRAGEN XY but f_stat < 0.3               → ambiguous
      - Otherwise pass through (XX, XY, XXY, …).

    The 1 < y_calls <= 5 gap on X0 falls through to pass-through (unusual;
    likely contamination — flagged by initial_karyotype mismatch upstream).
    """
    if initial_karyotype is None:
        return None
    if initial_karyotype == 'X0':
        if y_calls > 5:
            return 'XY'
        return 'X0'
    if initial_karyotype == 'XX' and not _isnan(f_stat) and f_stat > 0.7:
        return 'ambiguous'
    if initial_karyotype == 'XY' and not _isnan(f_stat) and f_stat < 0.3:
        return 'ambiguous'
    return initial_karyotype


def _isnan(x: float) -> bool:
    return x != x


def impute_sex_for_cohort(
    cohort_sgs: 'list[SequencingGroup]',
    *,
    median_correct: bool = False,
) -> dict[str, dict[str, Any]]:
    """Read somalier sketches + DRAGEN ploidy CSVs for each SG and compute
    per-sample sex imputation metrics.

    Returns a dict keyed by sg.id; each value contains:
        corrected_sex_karyotype: str | None
        f_stat:                  float
        x_het_rate:              float
        n_called_x:              int
        y_calls:                 int
        y_n:                     int

    Median correction (when enabled) renormalises f_stat by the cohort
    median chrX heterozygosity over putative XX samples (DRAGEN ploidy XX
    AND y_calls <= 1). Falls back to the simple proxy when fewer than
    MEDIAN_CORRECT_MIN_XX such samples are present.

    Sequencing groups missing either input file are skipped with a warning.
    """
    raw: dict[str, dict[str, Any]] = {}
    for sg in cohort_sgs:
        # somalier sketch is keyed by sg.id; DRAGEN dir is keyed by sg.name
        somalier_path = get_output_path(f'somalier/{sg.id}.somalier')
        ploidy_path = get_output_path(
            f'dragen_metrics/{sg.name}/{sg.name}.ploidy_estimation_metrics.csv',
        )
        try:
            sketch_bytes = somalier_path.read_bytes()
            ploidy_bytes = ploidy_path.read_bytes()
        except (FileNotFoundError, CloudPathFileNotFoundError) as e:
            logger.warning(f'Skipping sex imputation for {sg.id}: {e}')
            continue

        sketch = parse_somalier_sketch(sketch_bytes)
        ploidy = parse_dragen_ploidy(ploidy_bytes)
        raw[sg.id] = {**sketch, 'ploidy_estimation': ploidy['ploidy_estimation']}

    xx_median_het_rate = _maybe_xx_median(raw) if median_correct else None

    result: dict[str, dict[str, Any]] = {}
    for sg_id, s in raw.items():
        x_het_rate, f_stat = compute_f_stat(
            s['x_het'], s['x_hom_ref'], s['x_hom_alt'],
            xx_median_het_rate=xx_median_het_rate,
        )
        n_called_x = s['x_hom_ref'] + s['x_het'] + s['x_hom_alt']
        corrected = karyotype_from_signals(s['ploidy_estimation'], f_stat, s['y_calls'])
        result[sg_id] = {
            'corrected_sex_karyotype': corrected,
            'f_stat': f_stat,
            'x_het_rate': x_het_rate,
            'n_called_x': n_called_x,
            'y_calls': s['y_calls'],
            'y_n': s['y_n'],
        }
    return result


def _maybe_xx_median(raw: dict[str, dict[str, Any]]) -> float | None:
    """Cohort median chrX het rate over putative XX samples, or None when
    fewer than MEDIAN_CORRECT_MIN_XX such samples are present."""
    putative_xx_rates: list[float] = []
    for s in raw.values():
        n_called = s['x_hom_ref'] + s['x_het'] + s['x_hom_alt']
        if (
            s['ploidy_estimation'] == 'XX'
            and s['y_calls'] <= 1
            and n_called > 0
        ):
            putative_xx_rates.append(s['x_het'] / n_called)

    if len(putative_xx_rates) < MEDIAN_CORRECT_MIN_XX:
        logger.warning(
            f'median_correct requested but only {len(putative_xx_rates)} putative XX '
            f'samples found (need >= {MEDIAN_CORRECT_MIN_XX}); falling back to simple f-stat.',
        )
        return None

    xx_median = statistics.median(putative_xx_rates)
    logger.info(
        f'Using median-corrected f-stat: XX median het rate = '
        f'{xx_median:.4f} (n={len(putative_xx_rates)})',
    )
    return xx_median
