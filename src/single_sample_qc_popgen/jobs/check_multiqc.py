"""
Checks metrics in MultiQC output, based on thresholds in the qc_thresholds
config section.

Script can send a report to a Slack channel. To enable that, set SLACK_TOKEN
and SLACK_CHANNEL environment variables, and add desired Slack app into
a channel with:

/invite @<your-app-name>
"""
import json
import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Final, LiteralString

import cpg_utils
from cpg_flow.targets import Cohort
from cpg_utils import to_path
from cpg_utils.config import config_retrieve, get_config
from cpg_utils.slack import send_message
from loguru import logger
from metamist.graphql import gql, query

from single_sample_qc_popgen.constants import FAILURE_RATE_THRESHOLD, OURDNA_CONTROL, get_sequencing_type_profile
from single_sample_qc_popgen.jobs.register_qc_metamist import resolve_metric_sections
from single_sample_qc_popgen.jobs.sex_imputation import impute_sex_for_cohort
from single_sample_qc_popgen.utils import get_cohort_sequencing_type, load_json

REPORTED_SEX_QUERY = gql(
    """
    query MyQuery($cohortId: String!) {
        cohorts(id: {eq: $cohortId}) {
            sequencingGroups {
            id
            sample {
                externalId
                participant {
                    reportedSex
                    meta
                }
            }
        }
    }
}
""",
)

MUTATION_DEACTIVATE_SGS = gql(
    """
    mutation MyMutation($sequencingGroupsToDeactivate: [String!]!) {
        sequencingGroup {
            archiveSequencingGroups(sequencingGroupIds: $sequencingGroupsToDeactivate) {
            archived
            id
            }
        }
    }
"""
)

MUTATION_SEQUENCING_GROUP = gql(
    """
    mutation MyMutation($project: String!, $sequencingGroup: SequencingGroupMetaUpdateInput!) {
        sequencingGroup {
            updateSequencingGroup(project: $project, sequencingGroup: $sequencingGroup) {
                id
                meta
            }
        }
    }
    """
)

PORTAL_SEX_FIELD: Final = 'participant_portal_reported_sex'
REPORTED_SEX_FIELD: Final = 'reportedSex'
# Reported sex codes that map to a karyotype to compare DRAGEN's ploidy with: 1 -> XY, 2 -> XX.
# Any other code (0 = unknown, ...) gives nothing to compare against.
USABLE_SEX_CODES: Final = (1, 2)

# MultiQC general-stats metric names that the ploidy and chimera calculators read.
PLOIDY_ESTIMATION: Final = 'Ploidy estimation'
CHIMERIC_ALIGNMENTS: Final = 'Supplementary (chimeric) alignments'
TOTAL_ALIGNMENTS: Final = 'Total alignments'


@dataclass(frozen=True)
class ReportedSexInfo:
    """Reported sex of a cohort's sequencing groups, and which SGs are controls.

    Attributes:
        sex_by_sg: SG ID to reported sex (1 or 2), only for SGs with a usable value.
        discordant_by_sg: SG ID to its QC failure line, for SGs whose portal and
            reportedSex fields are both set and differ.
        control_sg_ids: SGs whose sample is an OurDNA control.
    """

    sex_by_sg: dict[str, int]
    discordant_by_sg: dict[str, str]
    control_sg_ids: set[str]


def resolve_reported_sex(portal_sex: int | None, reported_sex: int | None) -> tuple[int | None, str | None]:
    """Combines the participant's two reported-sex fields.

    The fields can disagree (e.g. portal 0 = unknown against reportedSex 1), so
    a value in one is never preferred over a different value in the other.

    Returns:
        (sex, discordant_line). sex is 1 or 2 when the fields agree on, or only
        one field holds, a usable code; None when neither is set or the code
        is not usable (e.g. 0). discordant_line is the QC failure line when both
        fields are set and differ (sex is then None), else None.
    """
    if portal_sex is not None and reported_sex is not None and portal_sex != reported_sex:
        return None, (
            f'Reported sex discordant ({PORTAL_SEX_FIELD}={portal_sex}, {REPORTED_SEX_FIELD}={reported_sex})'
        )
    sex = portal_sex if portal_sex is not None else reported_sex
    return (sex if sex in USABLE_SEX_CODES else None), None


def get_reported_sex_info(cohort: Cohort) -> ReportedSexInfo:
    """Looks up each SG's reported sex in metamist, and which SGs are controls.

    The sex comes from 'participant_portal_reported_sex' (participant.meta) and
    'reportedSex', combined by resolve_reported_sex. A participant with only
    'reportedSex' set is logged as a fallback from the portal field.
    """
    response = query(REPORTED_SEX_QUERY, variables={'cohortId': cohort.id})
    sex_by_sg: dict[str, int] = {}
    discordant_by_sg: dict[str, str] = {}
    control_sg_ids: set[str] = set()
    for coh in response['cohorts']:
        for sg in coh['sequencingGroups']:
            sg_id = sg['id']
            participant = sg['sample']['participant']
            if OURDNA_CONTROL in sg['sample']['externalId']:
                control_sg_ids.add(sg_id)

            participant_meta = participant.get('meta')
            portal_sex = participant_meta.get(PORTAL_SEX_FIELD) if isinstance(participant_meta, dict) else None
            reported_sex = participant.get(REPORTED_SEX_FIELD)
            if portal_sex is None and reported_sex is not None:
                logger.warning(
                    f"SG {sg_id}: Preferred field '{PORTAL_SEX_FIELD}' "
                    f"not found in meta. Using field '{REPORTED_SEX_FIELD}' as fallback."
                )
            sex, discordant_line = resolve_reported_sex(portal_sex, reported_sex)
            if sex is not None:
                sex_by_sg[sg_id] = sex
            if discordant_line is not None:
                discordant_by_sg[sg_id] = discordant_line
    return ReportedSexInfo(sex_by_sg=sex_by_sg, discordant_by_sg=discordant_by_sg, control_sg_ids=control_sg_ids)


class QCChecker:
    """
    Encapsulates all logic for checking a MultiQC report for a cohort.
    """
    # Now accepts multiqc_data directly, rather than finding it itself
    def __init__(self, cohort: Cohort, multiqc_data: dict, sequencing_type: str):
        profile = get_sequencing_type_profile(sequencing_type)
        mean_coverage = profile.mean_coverage
        median_coverage = profile.median_coverage
        pct_gt_20x = profile.pct_gt_20x
        self.cohort = cohort
        self.cohort_sgs = self.cohort.get_sequencing_groups()
        reported_sex = get_reported_sex_info(self.cohort)
        # Only SGs with a usable reported sex (1 or 2); the ploidy check applies to these alone.
        self.sex_mapping: dict[str, int] = reported_sex.sex_by_sg
        self.sex_discordant_by_sg: dict[str, str] = reported_sex.discordant_by_sg
        self.control_sg_ids: set[str] = reported_sex.control_sg_ids
        self.multiqc_data = multiqc_data
        # Per-SG raw somalier signals (f_stat_raw, x_het_rate, counters).
        # Karyotype derivation lives downstream in ourdna_genomic_atlas; this
        # repo emits facts only.
        self.sex_imputation_by_sg: dict[str, dict[str, Any]] = impute_sex_for_cohort(
            self.cohort_sgs,
        )
        # Each metric's source CSV must be in the profile's required_input_suffixes (constants.py),
        # because a report-only file may be missing for an SG.
        #
        # Each entry names the MultiQC general-stats metrics it reads: 'multiqc_report_name'
        # for a direct lookup, or 'calculator' (taking the SG's values of 'input_metric_names')
        # for a derived value. 'requires_reported_sex' marks a metric that is only checked
        # for SGs with a usable reported sex (see check_cohort_metrics).
        self.QC_MAPPING: dict[str, dict[str, Any]] = {
            mean_coverage.meta_key: {
                'multiqc_report_name': mean_coverage.multiqc_name,
                'display_name': mean_coverage.display_name,
            },
            median_coverage.meta_key: {
                'multiqc_report_name': median_coverage.multiqc_name,
                'display_name': median_coverage.display_name,
            },
            'ploidy_estimation': {
                'calculator': self._calculate_ploidy,
                'input_metric_names': [PLOIDY_ESTIMATION],
                'requires_reported_sex': True,
                'display_name': 'Ploidy Estimation',
            },
            pct_gt_20x.meta_key: {
                'multiqc_report_name': pct_gt_20x.multiqc_name,
                'display_name': pct_gt_20x.display_name,
            },
            'q30_bases': {
                'multiqc_report_name': 'Q30 bases',
                'display_name': 'Q30 Bases',
            },
            'q30_bases_pct': {
                'multiqc_report_name': 'Q30 bases pct',
                'display_name': 'Q30 Bases (%)',
            },
            'contamination_dragen': {
                'multiqc_report_name': 'Estimated sample contamination',
                'display_name': 'Contamination (DRAGEN)',
            },
            'mapping_rate_pct': {
                'multiqc_report_name': 'Mapped reads pct',
                'display_name': 'Mapping Rate (%)',
            },
            'duplication_rate_pct': {
                'multiqc_report_name': 'Number of duplicate marked reads pct',
                'display_name': 'Duplication Rate (%)',
            },
            'chimera_rate': {
                'calculator': self._calculate_chimera_rate,
                'input_metric_names': [CHIMERIC_ALIGNMENTS, TOTAL_ALIGNMENTS],
                'display_name': 'Chimera Rate',
            },
            'mean_insert_size': {
                'multiqc_report_name': 'Insert length: mean',
                'display_name': 'Mean Insert Size',
            },
            'insert_size_sd': {
                'multiqc_report_name': 'Insert length: standard deviation',
                'display_name': 'Insert Size SD',
            },
            'ti_tv_ratio': {
                'multiqc_report_name': 'Ti/Tv ratio',
                'display_name': 'Ti/Tv Ratio (SNPs)',
            },
            'het_hom_ratio': {
                'multiqc_report_name': 'Het/Hom ratio',
                'display_name': 'Het/Hom Ratio',
            },
        }

    def _calculate_ploidy(self, d: dict, sg_id: str, sex_mapping: dict[str, int]) -> tuple[bool | None, str, str]:
        """
        Validates that the DRAGEN-estimated ploidy matches the participant's reported sex.

        This method performs a strict string comparison between the DRAGEN output
        (e.g., 'XX', 'XY', 'XC', 'XXY') and the expected karyotype derived from
        the metadata sex code (1 -> 'XY', 2 -> 'XX').

        Any deviation from the strict expected string (including valid biological
        aneuploidies like 'XXY' or 'XO') will result in a mismatch (False) to flag
        the sample for manual review. Somalier-derived signals are advisory-only
        and live downstream; they do not contribute to this pass/fail decision.

        Only called for SGs whose reported sex is 1 or 2 (SGs without a usable
        reported sex are not checked, see check_cohort_metrics).

        Returns:
            tuple[bool | None, str, str]: A tuple containing:
                - is_match (bool | None): True if ploidy matches expected sex exactly,
                  False if there is a mismatch, or None if DRAGEN gave no ploidy.
                - raw_ploidy (str): The raw string value from DRAGEN (e.g., 'XX', 'Unknown').
                - expected_ploidy (str): The expected string ('XY' or 'XX').

        Raises:
            ValueError: If sg_id has no reported sex of 1 or 2 in sex_mapping.
        """
        raw_ploidy = d.get(PLOIDY_ESTIMATION, 'Unknown')

        expected_sex_num = sex_mapping.get(sg_id)
        if expected_sex_num not in USABLE_SEX_CODES:
            raise ValueError(f'{sg_id} has no usable reported sex ({expected_sex_num!r}); cannot check its ploidy')

        if raw_ploidy == 'Unknown':
            return None, raw_ploidy, str(expected_sex_num)

        expected_ploidy = 'XY' if expected_sex_num == 1 else 'XX'

        # Strict comparison check
        # This flags anything that isn't exactly XX or XY (e.g., XO, XXY, XYY, XXX)
        is_match = raw_ploidy == expected_ploidy

        return is_match, raw_ploidy, expected_ploidy

    def _calculate_chimera_rate(
        self, d: dict, _: str, __: dict
    ) -> tuple[float | None, str, str | None]:
        """
        Calculator for chimera rate.
        Returns: (value_to_check, raw_value_for_log, expected_value_for_log)
        """
        try:
            val = d[CHIMERIC_ALIGNMENTS] / d[TOTAL_ALIGNMENTS]
            # No raw value or expected value, so return val and None
            return val, f"{val:.4f}", None
        except (KeyError, ZeroDivisionError, TypeError):
            return None, "N/A", None


def build_qc_thresholds(seq_type: str, config_key: str, qc_checker: QCChecker) -> dict[str, dict]:
    """
    Build a dictionary of desired QC thresholds from config.
    Example config structure:
        [qc_thresholds.genome.min]
        mean_coverage = 30
        q30_bases_pct = 80
        [qc_thresholds.genome.max]
        contamination_dragen = 0.03
        chimera_rate = 0.05
        [qc_thresholds.genome.equality]
        ploidy_estimation = True

    Raises:
        RuntimeError: If seq_type has no thresholds at all (its block is missing,
            or its min/max/equality sub-blocks are all missing or empty), or if
            a threshold names a metric that is not in qc_checker.QC_MAPPING.
            A threshold on an unmapped metric would never be enforced, because
            no MultiQC value exists under an unmapped name. A missing
            min/max/equality sub-block alongside a non-empty one just means no
            thresholds of that kind.
    """
    thresholds_by_seq_type = get_config().get('qc_thresholds', {})
    thresholds_for_type = thresholds_by_seq_type.get(seq_type, {})
    if not any(thresholds_for_type.get(kind) for kind in ('min', 'max', 'equality')):
        raise RuntimeError(
            f'No thresholds under [qc_thresholds.{seq_type}] in config (min, max and equality are all missing '
            f'or empty); no QC thresholds would be applied to {seq_type} data. '
            f'Blocks present: {sorted(thresholds_by_seq_type)}'
        )
    threshold_d = thresholds_for_type.get(config_key, {})
    qc_thresholds = {}
    for metric, threshold in threshold_d.items():
        if metric not in qc_checker.QC_MAPPING:
            raise RuntimeError(
                f"[qc_thresholds.{seq_type}.{config_key}] sets '{metric}', which is not a valid metric for "
                f'{seq_type} and would never be enforced. Valid metrics: {sorted(qc_checker.QC_MAPPING)}'
            )
        qc_thresholds[metric] = {
            'threshold': threshold,
            **qc_checker.QC_MAPPING[metric],
        }
    return qc_thresholds

def metric_input_names(metric_config: dict[str, Any]) -> list[str]:
    """The MultiQC general-stats metric names a QC_MAPPING entry reads."""
    if 'input_metric_names' in metric_config:
        return metric_config['input_metric_names']
    return [metric_config['multiqc_report_name']]


def check_threshold_metrics_in_report(threshold_map: dict[str, dict[str, Any]], multiqc_data: dict) -> None:
    """Raises RuntimeError if a metric a threshold reads is in no general-stats section.

    A metric absent from the whole report means the wrong report or profile,
    not a sample lacking a value, and its threshold would never be applied.
    For calculated metrics this covers the raw metrics the calculator reads.
    """
    missing = sorted(
        {
            name
            for metric_config in threshold_map.values()
            for name in metric_input_names(metric_config)
            if not any(name in val_by_metric for section in multiqc_data.values() for val_by_metric in section.values())
        }
    )
    if missing:
        raise RuntimeError(
            f'Thresholded metrics missing from every MultiQC general-stats section: {missing}. '
            f'Sections present: {list(multiqc_data)}'
        )


def get_metric_inputs(
    multiqc_data: dict,
    section_by_metric: dict[str, str],
    sg_id: str,
    input_names: list[str],
) -> dict[str, Any]:
    """The SG's values of the named metrics, each read from the one section that holds it.

    A metric is left out when the SG has no value for it: the SG is absent
    from the section, the key is absent, or the value is None.
    """
    inputs: dict[str, Any] = {}
    for name in input_names:
        value = multiqc_data[section_by_metric[name]].get(sg_id, {}).get(name)
        if value is not None:
            inputs[name] = value
    return inputs


def is_missing(value: Any) -> bool:
    """True for None and for a non-finite float (NaN or +/-inf), which no threshold can judge."""
    return value is None or (isinstance(value, float) and not math.isfinite(value))


def get_metric_value(
        qc_checker: QCChecker,
        metric_config: dict,
        val_by_metric: dict,
        sg_id: str,
    ) -> tuple[Any, str, str | None]:
        """
        Gets the metric value, either from a calculator or direct lookup.
        val_by_metric holds the SG's values from get_metric_inputs.
        Returns: (value_to_check, raw_value_for_log, expected_value_for_log)
        """
        if 'calculator' in metric_config:
            # Use the calculator function
            return metric_config['calculator'](
                val_by_metric, sg_id, qc_checker.sex_mapping
            )

        # Default: Direct lookup
        val = val_by_metric.get(metric_config['multiqc_report_name'])
        return val, str(val), None

def format_log_line(
        display_name: str,
        val_to_check: Any,
        threshold: Any,
        sign: str,
        check_type: str,
        raw_val_for_log: str,
        expected_val_for_log: str | None,
    ) -> str:
        """Formats the log line based on the check type."""
        if check_type == 'equality' and isinstance(val_to_check, bool):
            # Special format for boolean checks (like ploidy)
            return (
                f'{display_name} is {raw_val_for_log} '
                f'(expected {expected_val_for_log})'
            )
        # Standard format for numeric checks
        try:
            return f'{display_name}={val_to_check:.4f} {sign} {threshold:.4f}'
        except (ValueError, TypeError):
            # Fallback for non-numeric values
            return f'{display_name}={val_to_check} {sign} {threshold}'

def write_qc_failures_to_json(
    bad_lines_by_sample: dict[str, list[str]],
    output: cpg_utils.Path,
) -> None:
    """Writes failed sample logs to a JSON file: ``{sg_id: [msg, ...]}``."""
    logger.info(f'Writing {len(bad_lines_by_sample)} failed sample(s) to {output}')
    with to_path(output).open('w') as f:
        json.dump(bad_lines_by_sample, f, indent=4)


def write_sex_imputation_to_json(
    sex_imputation_by_sg: dict[str, dict[str, Any]],
    output: cpg_utils.Path,
) -> None:
    """Writes per-SG sex-imputation results to a JSON file: ``{sg_id: {...}}``."""
    logger.info(
        f'Writing {len(sex_imputation_by_sg)} sex-imputation record(s) to {output}',
    )
    with to_path(output).open('w') as f:
        json.dump(sex_imputation_by_sg, f, indent=4)

def generate_slack_message(
    bad_lines_by_sample: dict[str, list[str]],
    qc_checker: QCChecker,
    html_url: str,
    ploidy_not_checked: Sequence[str],
) -> LiteralString:
    """Constructs the final Slack message.

    Control samples (e.g. NA12878) are reported in a separate, informational
    block and excluded from the flagged count and high-failure-rate alert:
    they are not topped up to yield, so their coverage failures are expected
    and should not read as real QC failures.

    SGs without a usable reported sex are not failures; the ploidy_not_checked
    SG IDs are listed in an informational line so they are not overlooked.
    """
    control_sg_ids = qc_checker.control_sg_ids

    # Partition failures: real failures drive the alert; control failures are informational only.
    real_failures = {sg: lines for sg, lines in bad_lines_by_sample.items() if sg not in control_sg_ids}
    control_failures = {sg: lines for sg, lines in bad_lines_by_sample.items() if sg in control_sg_ids}

    num_failed = len(real_failures)
    # Denominator excludes controls too, so the rate reflects real samples only.
    num_total_sgs = len(qc_checker.cohort_sgs) - len(control_sg_ids)

    # 1. Check for high failure rate (controls excluded from both sides)
    high_failure_message = None
    if num_total_sgs > 0 and (num_failed / num_total_sgs) > FAILURE_RATE_THRESHOLD:
        failure_percent = (num_failed / num_total_sgs) * 100
        high_failure_message = (
            '=================================\n'
            '🚨 ALERT: High QC Failure Rate 🚨\n'
            '=================================\n'
            f'*Failure Rate:* {num_failed} out of {num_total_sgs} samples ({failure_percent:.2f}%)\n\n'
        )

    # 2. Construct the main message
    title = f'*[{qc_checker.cohort.id}]* <{html_url}|{"MultiQC report"}>'
    messages = []

    if high_failure_message:
        messages.append(high_failure_message)

    if num_failed > 0:
        messages.append(f'{title}. {num_failed} samples are flagged:')
        for sg_id, bad_lines in real_failures.items():
            messages.append(f'❗ {sg_id}: ' + ', '.join(bad_lines))
    else:
        messages.append(f'✅ {title}')

    # 3. Informational: SGs whose ploidy could not be checked for lack of a reported sex
    if ploidy_not_checked:
        messages.append(
            f'ℹ️ Ploidy not checked (no reported sex): {len(ploidy_not_checked)} SGs: '  # noqa: RUF001
            + ', '.join(ploidy_not_checked)
        )

    # 4. Informational control block
    if control_failures:
        messages.append('\n— Controls —')
        for sg_id, bad_lines in control_failures.items():
            messages.append(f'ℹ️ {sg_id}: ' + ', '.join(bad_lines))  # noqa: RUF001

    text: LiteralString = '\n'.join(messages)
    logger.info(text)

    return text


# (config key, sign shown on failure, sign shown on pass, is_fail(value, threshold))
CHECK_DEFINITIONS: Final = [
    ('min', '<', '≥', lambda val, thresh: val < thresh),
    ('max', '>', '≤', lambda val, thresh: val > thresh),
    ('equality', '!=', '==', lambda val, thresh: val != thresh),
]


@dataclass(frozen=True)
class QCResults:
    """Outcome of check_cohort_metrics.

    Attributes:
        bad_lines_by_sample: SG ID to its QC failure lines; SGs that pass are absent.
        ploidy_not_checked: Sorted IDs of non-control SGs whose ploidy was not checked
            because they have no usable reported sex. Not failures.
    """

    bad_lines_by_sample: dict[str, list[str]]
    ploidy_not_checked: list[str]


def record_failure(bad_lines_by_sample: dict[str, list[str]], sg_id: str, line: str) -> None:
    """Logs a QC failure and adds it to the SG's lines, once per distinct line."""
    if line in bad_lines_by_sample.get(sg_id, []):
        return
    logger.warning(f'❗ {sg_id}: {line}')
    bad_lines_by_sample.setdefault(sg_id, []).append(line)


def check_sg_metric(
    qc_checker: QCChecker,
    section_by_metric: dict[str, str],
    sg_id: str,
    metric_config: dict[str, Any],
    check_definition: tuple[str, str, str, Any],
    bad_lines_by_sample: dict[str, list[str]],
) -> None:
    """Checks one SG against one threshold, recording a failure line if it fails.

    A metric the SG has no value for (absent, None, NaN, or not computable by
    the calculator from missing or zero inputs) is a failure, '<display name> missing',
    because an unchecked SG would otherwise pass unnoticed.
    """
    check_type, fail_sign, good_sign, is_fail = check_definition
    display_name = metric_config['display_name']

    # DRAGEN does not provide pct chimeras directly, so we calculate it
    # Also, ploidy estimation needs custom calculation
    inputs = get_metric_inputs(
        qc_checker.multiqc_data, section_by_metric, sg_id, metric_input_names(metric_config)
    )
    val_to_check, raw_val_for_log, expected_val_for_log = get_metric_value(qc_checker, metric_config, inputs, sg_id)

    if is_missing(val_to_check):
        record_failure(bad_lines_by_sample, sg_id, f'{display_name} missing')
        return

    threshold = metric_config['threshold']
    failed = is_fail(val_to_check, threshold)
    line = format_log_line(
        display_name,
        val_to_check,
        threshold,
        fail_sign if failed else good_sign,
        check_type,
        raw_val_for_log,
        expected_val_for_log,
    )
    if failed:
        record_failure(bad_lines_by_sample, sg_id, line)
    else:
        logger.info(f'✅ {sg_id}: {line}')


def check_cohort_metrics(qc_checker: QCChecker, seq_type: str) -> QCResults:
    """Checks every cohort SG against every configured threshold.

    Each metric's value is read from the one MultiQC section that holds it
    (resolve_metric_sections), for every SG in the cohort, so an SG with no value
    fails with '<display name> missing' instead of being skipped.

    Reported sex gates the ploidy check:
      * Portal and reportedSex fields both set and different: the SG fails with a
        'Reported sex discordant' line, and its ploidy is not checked.
      * No usable reported sex (neither field set, or only 0): ploidy is not checked
        and the SG does not fail. Non-control SGs are returned in ploidy_not_checked.
        Controls are left out because their sex is never recorded.
    Both only apply when a threshold on a metric needing the reported sex is configured.

    Raises:
        RuntimeError: If a thresholded metric is in no MultiQC section, or there are
            no thresholds for seq_type (see build_qc_thresholds).
        ValueError: If a thresholded metric is in more than one MultiQC section.
    """
    multiqc_data = qc_checker.multiqc_data
    threshold_maps = {
        check_definition[0]: build_qc_thresholds(seq_type, check_definition[0], qc_checker)
        for check_definition in CHECK_DEFINITIONS
    }
    for threshold_map in threshold_maps.values():
        check_threshold_metrics_in_report(threshold_map, multiqc_data)
    section_by_metric = resolve_metric_sections(
        multiqc_data,
        sorted(
            {
                name
                for threshold_map in threshold_maps.values()
                for metric_config in threshold_map.values()
                for name in metric_input_names(metric_config)
            }
        ),
    )

    bad_lines_by_sample: dict[str, list[str]] = {}
    ploidy_not_checked: list[str] = []
    if any(
        metric_config.get('requires_reported_sex')
        for threshold_map in threshold_maps.values()
        for metric_config in threshold_map.values()
    ):
        for sg in qc_checker.cohort_sgs:
            if sg.id in qc_checker.sex_discordant_by_sg:
                record_failure(bad_lines_by_sample, sg.id, qc_checker.sex_discordant_by_sg[sg.id])
            elif sg.id not in qc_checker.sex_mapping and sg.id not in qc_checker.control_sg_ids:
                ploidy_not_checked.append(sg.id)
        ploidy_not_checked.sort()
        if ploidy_not_checked:
            logger.warning(
                f'Ploidy not checked for {len(ploidy_not_checked)} SGs with no usable reported sex '
                f"(neither '{PORTAL_SEX_FIELD}' nor '{REPORTED_SEX_FIELD}' is 1 or 2): {ploidy_not_checked}"
            )

    for check_definition in CHECK_DEFINITIONS:
        for sg in qc_checker.cohort_sgs:
            for metric_config in threshold_maps[check_definition[0]].values():
                if metric_config.get('requires_reported_sex') and sg.id not in qc_checker.sex_mapping:
                    # Discordant SGs already failed and the others are listed above.
                    continue
                check_sg_metric(
                    qc_checker, section_by_metric, sg.id, metric_config, check_definition, bad_lines_by_sample
                )

    return QCResults(bad_lines_by_sample=bad_lines_by_sample, ploidy_not_checked=ploidy_not_checked)


def run(
    cohort: Cohort,
    multiqc_data_path: str,
    multiqc_html_path: str,
    failures_output: cpg_utils.Path,
    sex_imputation_output: cpg_utils.Path,
):

    if base_url := cohort.dataset.web_url():
        # Construct HTML URL viewable in browser
        html_url = str(multiqc_html_path).replace(str(cohort.dataset.web_prefix()), base_url)
        logger.info(f'MultiQC report URL: {html_url}')

    multiqc_data = load_json(
            multiqc_data_path,
            extract_key='report_general_stats_data'
        )
    seq_type = get_cohort_sequencing_type(cohort)
    qc_checker = QCChecker(cohort, multiqc_data, seq_type)

    results = check_cohort_metrics(qc_checker, seq_type)
    bad_lines_by_sample = results.bad_lines_by_sample

    logger.info('') # Newline for readability

    # --- Post-checking steps ---
    # Both files are always written: register_qc_metamist depends on the
    # sex-imputation file even when no samples fail QC, and writing an empty
    # failures dict keeps the CheckMultiQc stage outputs satisfied.
    write_qc_failures_to_json(bad_lines_by_sample, failures_output)
    write_sex_imputation_to_json(qc_checker.sex_imputation_by_sg, sex_imputation_output)

    if config_retrieve(key=['workflow', 'send_to_slack'], default=True):
        text: LiteralString = generate_slack_message(
            bad_lines_by_sample, qc_checker, html_url, results.ploidy_not_checked
        )
        send_message(text)
    else:
        logger.info('Skipping Slack notification as per config.')
