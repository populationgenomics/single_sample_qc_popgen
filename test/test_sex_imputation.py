"""
Unit tests for somalier-based sex imputation pure logic.

Synthetic somalier sketches are constructed via ``_make_sketch`` to verify
the binary parser, f-stat formula, and karyotype rules with deterministic
inputs. Counts are intentionally small and contrived — the goal is to
exercise the format and the math, not to mirror real cohort samples.
"""
import math
import struct

import pytest

from single_sample_qc_popgen.jobs.sex_imputation import (
    compute_f_stat,
    parse_somalier_sketch,
)


def _make_sketch(
    name: str = 'TEST',
    n_auto: int = 5,
    x_hom_ref: int = 0,
    x_het: int = 0,
    x_hom_alt: int = 0,
    x_no_call: int = 0,
    y_calls: int = 0,
    y_no_call: int = 0,
) -> bytes:
    """Build a synthetic somalier sketch with the given genotype counts.

    Site records use a fixed encoding:
      hom_ref → (10, 0, 0), het → (10, 10, 0), hom_alt → (0, 10, 0),
      no_call → (0, 0, 0). Autosomal records are arbitrary and skipped
      by the parser.
    """
    name_bytes = name.encode('utf-8')
    n_x = x_hom_ref + x_het + x_hom_alt + x_no_call
    n_y = y_calls + y_no_call

    out = struct.pack('<BB', 2, len(name_bytes)) + name_bytes
    out += struct.pack('<HHH', n_auto, n_x, n_y)
    out += struct.pack('<III', 10, 10, 0) * n_auto
    out += struct.pack('<III', 10, 0, 0) * x_hom_ref
    out += struct.pack('<III', 10, 10, 0) * x_het
    out += struct.pack('<III', 0, 10, 0) * x_hom_alt
    out += struct.pack('<III', 0, 0, 0) * x_no_call
    out += struct.pack('<III', 10, 0, 0) * y_calls
    out += struct.pack('<III', 0, 0, 0) * y_no_call
    return out


# ---------------------------------------------------------------------------
# parse_somalier_sketch — verify the binary parser
# ---------------------------------------------------------------------------

class TestParseSomalierSketch:
    def test_extracts_genotype_counts(self):
        # Distinct counts in each bucket — verifies bucket boundaries
        data = _make_sketch(
            x_hom_ref=7, x_het=3, x_hom_alt=5,
            y_calls=4, y_no_call=2,
        )
        assert parse_somalier_sketch(data) == {
            'x_hom_ref': 7, 'x_het': 3, 'x_hom_alt': 5, 'x_n': 15,
            'y_calls': 4, 'y_n': 6,
        }

    def test_no_calls_in_x_n_but_excluded_from_genotype_counts(self):
        # x_n is the total chrX site count (denominator); the genotype
        # counts only sum sites with at least one read.
        x_hom_ref, x_het, x_hom_alt, x_no_call = 10, 5, 15, 20
        y_calls_in, y_no_call = 2, 10
        data = _make_sketch(
            x_hom_ref=x_hom_ref, x_het=x_het, x_hom_alt=x_hom_alt, x_no_call=x_no_call,
            y_calls=y_calls_in, y_no_call=y_no_call,
        )
        result = parse_somalier_sketch(data)
        assert result['x_n'] == x_hom_ref + x_het + x_hom_alt + x_no_call
        assert result['x_hom_ref'] + result['x_het'] + result['x_hom_alt'] == x_hom_ref + x_het + x_hom_alt
        assert result['y_n'] == y_calls_in + y_no_call
        assert result['y_calls'] == y_calls_in

    def test_handles_variable_name_length(self):
        # Header skips the variable-length sample name correctly
        data_short = _make_sketch(name='A', x_hom_ref=2, x_het=1, x_hom_alt=1)
        data_long = _make_sketch(name='SAMPLE_WITH_LONG_NAME_42', x_hom_ref=2, x_het=1, x_hom_alt=1)
        assert parse_somalier_sketch(data_short) == parse_somalier_sketch(data_long)

    def test_empty_y_block(self):
        # Edge case: zero chrY sites
        data = _make_sketch(x_hom_ref=3, x_het=2, x_hom_alt=5)
        result = parse_somalier_sketch(data)
        assert result['y_n'] == 0
        assert result['y_calls'] == 0


# ---------------------------------------------------------------------------
# compute_f_stat — verify the raw f-stat formula with clean fractions
# ---------------------------------------------------------------------------

class TestComputeFStat:
    def test_high_het_rate_low_f_stat(self):
        # het_rate = 4/10 = 0.4 → f_stat_raw = 1 - 0.8 = 0.2 (XX-like)
        het_rate, f_stat_raw = compute_f_stat(x_het=4, x_hom_ref=3, x_hom_alt=3)
        assert het_rate == pytest.approx(0.4)
        assert f_stat_raw == pytest.approx(0.2)

    def test_zero_het_rate_max_f_stat(self):
        # het_rate = 0 → f_stat_raw = 1 (XY-like, no chrX hets)
        het_rate, f_stat_raw = compute_f_stat(x_het=0, x_hom_ref=5, x_hom_alt=5)
        assert het_rate == 0.0
        assert f_stat_raw == 1.0

    def test_low_het_rate_high_f_stat(self):
        # het_rate = 1/10 = 0.1 → f_stat_raw = 1 - 0.2 = 0.8 (XY-like)
        _, f_stat_raw = compute_f_stat(x_het=1, x_hom_ref=4, x_hom_alt=5)
        assert f_stat_raw == pytest.approx(0.8)

    def test_zero_calls_returns_nan(self):
        het_rate, f_stat_raw = compute_f_stat(0, 0, 0)
        assert math.isnan(het_rate)
        assert math.isnan(f_stat_raw)
