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
    MEDIAN_CORRECT_MIN_XX,
    _maybe_xx_median,
    compute_f_stat,
    karyotype_from_signals,
    parse_dragen_ploidy,
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
        data = _make_sketch(
            x_hom_ref=10, x_het=5, x_hom_alt=15, x_no_call=20,
            y_calls=2, y_no_call=10,
        )
        result = parse_somalier_sketch(data)
        assert result['x_n'] == 50
        assert result['x_hom_ref'] + result['x_het'] + result['x_hom_alt'] == 30
        assert result['y_n'] == 12
        assert result['y_calls'] == 2

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
# compute_f_stat — verify the f-stat formula with clean fractions
# ---------------------------------------------------------------------------

class TestComputeFStat:
    def test_high_het_rate_low_f_stat(self):
        # het_rate = 4/10 = 0.4 → f_stat = 1 - 0.8 = 0.2 (XX-like)
        het_rate, f_stat = compute_f_stat(x_het=4, x_hom_ref=3, x_hom_alt=3)
        assert het_rate == pytest.approx(0.4)
        assert f_stat == pytest.approx(0.2)

    def test_zero_het_rate_max_f_stat(self):
        # het_rate = 0 → f_stat = 1 (XY-like, no chrX hets)
        het_rate, f_stat = compute_f_stat(x_het=0, x_hom_ref=5, x_hom_alt=5)
        assert het_rate == 0.0
        assert f_stat == 1.0

    def test_low_het_rate_high_f_stat(self):
        # het_rate = 1/10 = 0.1 → f_stat = 1 - 0.2 = 0.8 (XY-like)
        _, f_stat = compute_f_stat(x_het=1, x_hom_ref=4, x_hom_alt=5)
        assert f_stat == pytest.approx(0.8)

    def test_zero_calls_returns_nan(self):
        het_rate, f_stat = compute_f_stat(0, 0, 0)
        assert math.isnan(het_rate)
        assert math.isnan(f_stat)

    def test_median_correction_centres_xx(self):
        # When het_rate equals the cohort XX median, corrected f_stat = 0
        _, f_stat = compute_f_stat(
            x_het=32, x_hom_ref=34, x_hom_alt=34, xx_median_het_rate=0.32,
        )
        assert f_stat == pytest.approx(0.0, abs=1e-6)

    def test_median_correction_no_hets_yields_one(self):
        # All-hom male: het_rate = 0 → corrected f_stat = 1
        _, f_stat = compute_f_stat(
            x_het=0, x_hom_ref=100, x_hom_alt=0, xx_median_het_rate=0.32,
        )
        assert f_stat == pytest.approx(1.0, abs=1e-6)


# ---------------------------------------------------------------------------
# karyotype_from_signals — verify the rule table
# ---------------------------------------------------------------------------

class TestKaryotypeFromSignals:
    def test_loy_x0_with_y_signal_becomes_xy(self):
        # DRAGEN called X0 but somalier sees clear Y signal → loss-of-Y
        assert karyotype_from_signals('X0', f_stat=0.95, y_calls=16) == 'XY'

    def test_true_x0_no_y_signal(self):
        assert karyotype_from_signals('X0', f_stat=0.5, y_calls=0) == 'X0'

    def test_x0_with_borderline_y_signal_passes_through(self):
        # 1 < y_calls <= 5 — unusual; not promoted to XY
        assert karyotype_from_signals('X0', f_stat=0.5, y_calls=3) == 'X0'

    def test_xx_with_male_fstat_is_ambiguous(self):
        assert karyotype_from_signals('XX', f_stat=0.95, y_calls=15) == 'ambiguous'

    def test_xy_with_female_fstat_is_ambiguous(self):
        assert karyotype_from_signals('XY', f_stat=0.20, y_calls=0) == 'ambiguous'

    def test_xx_passthrough(self):
        assert karyotype_from_signals('XX', f_stat=0.35, y_calls=0) == 'XX'

    def test_xy_passthrough(self):
        assert karyotype_from_signals('XY', f_stat=0.96, y_calls=16) == 'XY'

    def test_xxy_passthrough(self):
        assert karyotype_from_signals('XXY', f_stat=0.6, y_calls=15) == 'XXY'

    def test_none_returns_none(self):
        assert karyotype_from_signals(None, f_stat=0.5, y_calls=0) is None

    def test_nan_fstat_does_not_trigger_ambiguous(self):
        assert karyotype_from_signals('XX', f_stat=float('nan'), y_calls=0) == 'XX'
        assert karyotype_from_signals('XY', f_stat=float('nan'), y_calls=16) == 'XY'


# ---------------------------------------------------------------------------
# parse_dragen_ploidy
# ---------------------------------------------------------------------------

class TestParseDragenPloidy:
    def test_xx_metrics(self):
        csv = (
            'PLOIDY ESTIMATION,,Autosomal median coverage,30.5\n'
            'PLOIDY ESTIMATION,,X median / Autosomal median,0.94\n'
            'PLOIDY ESTIMATION,,Y median / Autosomal median,0.0\n'
            'PLOIDY ESTIMATION,,Ploidy estimation,XX\n'
        )
        assert parse_dragen_ploidy(csv.encode('utf-8')) == {
            'ploidy_estimation': 'XX',
            'norm_x_coverage': 0.94,
            'norm_y_coverage': 0.0,
        }

    def test_xy_metrics(self):
        csv = (
            'PLOIDY ESTIMATION,,X median / Autosomal median,0.5\n'
            'PLOIDY ESTIMATION,,Y median / Autosomal median,0.45\n'
            'PLOIDY ESTIMATION,,Ploidy estimation,XY\n'
        )
        result = parse_dragen_ploidy(csv.encode('utf-8'))
        assert result['ploidy_estimation'] == 'XY'
        assert result['norm_x_coverage'] == 0.5
        assert result['norm_y_coverage'] == 0.45

    def test_missing_fields_return_none(self):
        csv = 'PLOIDY ESTIMATION,,Some other field,42\n'
        result = parse_dragen_ploidy(csv.encode('utf-8'))
        assert result['ploidy_estimation'] is None
        assert result['norm_x_coverage'] is None
        assert result['norm_y_coverage'] is None


# ---------------------------------------------------------------------------
# _maybe_xx_median (cohort-level guard)
# ---------------------------------------------------------------------------

# x_het / n_called = 30/100 = 0.30 — a typical XX het rate
_XX_RAW = {
    'x_hom_ref': 35, 'x_het': 30, 'x_hom_alt': 35,
    'ploidy_estimation': 'XX', 'y_calls': 0,
}


class TestMaybeXxMedian:
    def test_returns_median_when_threshold_met(self):
        raw = {f'sg{i}': dict(_XX_RAW) for i in range(MEDIAN_CORRECT_MIN_XX)}
        assert _maybe_xx_median(raw) == pytest.approx(0.30, abs=1e-6)

    def test_falls_back_below_threshold(self):
        raw = {f'sg{i}': dict(_XX_RAW) for i in range(MEDIAN_CORRECT_MIN_XX - 1)}
        assert _maybe_xx_median(raw) is None

    def test_xy_samples_excluded(self):
        xy = {
            'x_hom_ref': 100, 'x_het': 5, 'x_hom_alt': 100,
            'ploidy_estimation': 'XY', 'y_calls': 16,
        }
        raw = {f'sg{i}': dict(xy) for i in range(20)}
        assert _maybe_xx_median(raw) is None

    def test_xx_with_y_signal_excluded(self):
        # Putative XX must have y_calls <= 1 to be considered "clean" XX
        contaminated = dict(_XX_RAW)
        contaminated['y_calls'] = 5
        raw = {f'sg{i}': dict(contaminated) for i in range(20)}
        assert _maybe_xx_median(raw) is None
