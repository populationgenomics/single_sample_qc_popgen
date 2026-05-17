"""
Pure logic for somalier-based sex imputation.

Files are read via cpg_utils.Path so this module can be invoked from inside
or outside a Hail Batch job.

The somalier sketch is a binary file produced by `somalier extract` upstream
in dragen_align_pa. Header layout (little-endian):
    version (u8), name_len (u8), name (utf-8, name_len bytes),
    n_auto (u16), n_x (u16), n_y (u16),
followed by (n_auto + n_x + n_y) site records of 12 bytes each:
    nref (u32), nalt (u32), nother (u32).

This module emits raw signals only — `f_stat_raw` (the simple ``1 - 2 *
x_het_rate`` proxy), chrX/chrY counters. It does NOT derive a karyotype.
Karyotype derivation (LoY rescue, raw-f-stat ambiguous gate) lives downstream
in ourdna_genomic_atlas alongside the per-ancestry f-stat renormalisation
that depends on the same raw signals.
"""

import struct
from typing import TYPE_CHECKING, Any

from cloudpathlib.exceptions import CloudPathFileNotFoundError
from loguru import logger

from single_sample_qc_popgen.utils import get_dragen_output_path

if TYPE_CHECKING:
    from cpg_flow.targets import SequencingGroup


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


def impute_sex_for_cohort(
    cohort_sgs: 'list[SequencingGroup]',
) -> dict[str, dict[str, Any]]:
    """Read somalier sketches for each SG and emit raw sex-imputation signals.

    Returns a dict keyed by sg.id; each value contains:
        f_stat_raw:  float
        x_het_rate:  float
        n_called_x:  int
        y_calls:     int
        y_n:         int

    Sequencing groups missing the somalier sketch are skipped with a warning.
    Karyotype derivation (LoY rescue, ambiguous gate) is the responsibility
    of downstream consumers (ourdna_genomic_atlas.ImputeSex), where the
    ancestry-aware f-stat renormalisation that shares the same raw signals
    also lives.
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
        result[sg.id] = {
            'f_stat_raw': f_stat_raw,
            'x_het_rate': x_het_rate,
            'n_called_x': n_called_x,
            'y_calls': sketch['y_calls'],
            'y_n': sketch['y_n'],
        }
    return result
