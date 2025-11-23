"""
Checks metrics in MultiQC output, based on thresholds in the qc_thresholds
config section.

Script can send a report to a Slack channel. To enable that, set SLACK_TOKEN
and SLACK_CHANNEL environment variables, and add desired Slack app into
a channel with:

/invite @<your-app-name>
"""
import json
from collections import defaultdict
from typing import Any

import cpg_utils
from cpg_flow.targets import Cohort
from cpg_utils import to_path
from cpg_utils.config import config_retrieve, get_config
from cpg_utils.slack import send_message
from loguru import logger
from metamist.graphql import gql, query

from single_sample_qc_popgen.constants import FAILURE_RATE_THRESHOLD
from single_sample_qc_popgen.utils import load_json

REPORTED_SEX_QUERY = gql(
    """
    query MyQuery($cohortId: String!) {
        cohorts(id: {eq: $cohortId}) {
            sequencingGroups {
            id
            sample {
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


def get_sgid_reported_sex_mapping(cohort: Cohort) -> dict[str, int]:
    """
    Get a mapping of sequencing group ID to reported sex.
    Preferentially uses 'participant_portal_reported_sex' from the
    participant.meta field, and falls back to 'reportedSex'.
    """
    mapping: dict[str, int] = {}
    response = query(REPORTED_SEX_QUERY, variables={'cohortId': cohort.id})
    for coh in response['cohorts']:
        for sg in coh['sequencingGroups']:
            sg_id = sg['id']
            participant = sg['sample']['participant']

            preferred_field = None
            participant_meta = participant.get('meta')

            # 1. Check for the preferred field first
            if isinstance(participant_meta, dict):
                preferred_field = participant_meta.get('participant_portal_reported_sex')

            # 2. If the preferred field exists, use it
            if preferred_field is not None:
                mapping[sg_id] = preferred_field

            # 3. If not, try the fallback field
            else:
                fallback_field = participant.get('reportedSex')
                if fallback_field is not None:
                    mapping[sg_id] = fallback_field
                    logger.warning(
                        f"SG {sg_id}: Preferred field 'participant_portal_reported_sex' "
                        f"not found in meta. Using field 'reportedSex' as fallback."
                    )
                else:
                    # 4. If both are missing, log an error
                    logger.error(
                        f"SG {sg_id}: CANNOT FIND SEX. Both 'participant_portal_reported_sex' "
                        f"and 'reportedSex' are missing or null. This SG will be "
                        f"missing from the sex map."
                    )
    return mapping


class QCChecker:
    """
    Encapsulates all logic for checking a MultiQC report for a cohort.
    """
    # Now accepts multiqc_data directly, rather than finding it itself
    def __init__(self, cohort: Cohort, multiqc_data: dict, output: cpg_utils.Path):
        self.cohort = cohort
        self.output = output
        self.cohort_sgs = self.cohort.get_sequencing_groups()
        self.sex_mapping = get_sgid_reported_sex_mapping(self.cohort)
        self.multiqc_data = multiqc_data
        self.QC_MAPPING: dict[str, dict[str, Any]] = {
            'mean_coverage': {
                'multiqc_report_name': 'Average sequenced coverage over genome',
                'display_name': 'Mean Coverage',
            },
            'ploidy_estimation': {
                'calculator': self._calculate_ploidy,
                'multiqc_report_name': 'Ploidy estimation',
                'display_name': 'Ploidy Estimation',
            },
            'pct_genome_gt_20x': {
                'multiqc_report_name': 'wgs pct of genome with coverage [20x:inf)',
                'display_name': 'Pct Genome @ >20x',
            },
            'q30_bases': {
                'multiqc_report_name': 'Q30 bases',
                'display_name': 'Q30 Bases',
            },
            'q30_bases_pct': {
                'multiqc_report_name': 'Q30 bases pct',
                'display_name': 'Q30 Bases (%)',
            },
            'contamination_verifybamid': {
                'multiqc_report_name': 'FREEMIX',
                'display_name': 'Contamination (VerifyBamID)',
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
        the sample for manual review.

        Returns:
            tuple[bool | None, str, str]: A tuple containing:
                - is_match (bool | None): True if ploidy matches expected sex exactly,
                  False if there is a mismatch, or None if metadata/metrics are missing.
                - raw_ploidy (str): The raw string value from DRAGEN (e.g., 'XX', 'Unknown').
                - expected_ploidy (str): The expected string (e.g., 'XY', 'XX') or error msg.
        """
        raw_ploidy = d.get('Ploidy estimation', 'Unknown')
        expected_sex_num = sex_mapping.get(sg_id)

        if expected_sex_num is None:
            return None, raw_ploidy, f"Unknown (no sex for {sg_id})"

        if raw_ploidy == 'Unknown':
            return None, raw_ploidy, str(expected_sex_num)

        expected_ploidy = 'XY' if expected_sex_num == 1 else 'XX'

        # Handle cases where sex is neither 1 nor 2 (e.g. 0/Unknown)
        if expected_sex_num not in [1, 2]:
             return None, raw_ploidy, f"Ambiguous Sex Code {expected_sex_num}"

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
            val = d['Supplementary (chimeric) alignments'] / d['Total alignments']
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
    """
    threshold_d = get_config()['qc_thresholds'].get(seq_type, {}).get(config_key, {})
    qc_thresholds = {}
    for metric, threshold in threshold_d.items():
        if metric in qc_checker.QC_MAPPING:
            qc_thresholds[metric] = {
                'threshold': threshold,
                **qc_checker.QC_MAPPING[metric],
            }
        else:
            logger.warning(
                f"Metric '{metric}' has a threshold but is not defined in QC_MAPPING. "
                f"Using default names."
                f"Default names are the following: {list(qc_checker.QC_MAPPING.keys())}"
            )
            qc_thresholds[metric] = {
                'threshold': threshold,
                'multiqc_report_name': metric,
                'display_name': metric,
            }
    return qc_thresholds

def get_metric_value(
        qc_checker: QCChecker,
        metric_config: dict,
        val_by_metric: dict,
        sg_id: str,
    ) -> tuple[Any, str, str | None]:
        """
        Gets the metric value, either from a calculator or direct lookup.
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

def write_failures_to_json(bad_lines_by_sample: dict[str, list[str]], output: cpg_utils.Path) -> None:
        """Writes all failed sample logs to a JSON file."""
        logger.warning(
            f'Writing {len(bad_lines_by_sample)} failed sample(s) to {output}'
        )
        with to_path(output).open('w') as f:
                json.dump(bad_lines_by_sample, f, indent=4)

def post_to_slack(bad_lines_by_sample: dict[str, list[str]], qc_checker: QCChecker, html_url: str) -> None:
    """Constructs and sends the final Slack message."""

    num_failed = len(bad_lines_by_sample)
    num_total_sgs = len(qc_checker.cohort_sgs)

    # 1. Check for high failure rate
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
        for sg_id, bad_lines in bad_lines_by_sample.items():
            messages.append(f'❗ {sg_id}: ' + ', '.join(bad_lines))
    else:
        messages.append(f'✅ {title}')

    text = '\n'.join(messages)
    logger.info(text)

    # 3. Send to Slack if enabled
    if config_retrieve(['workflow', 'multiqc', 'send_to_slack'], default=True):
        send_message(text)
    else:
        logger.info('Skipping Slack notification as per config.')


def run(
    cohort: Cohort,
    multiqc_data_path: str,
    multiqc_html_path: str,
    output: cpg_utils.Path,
):

    if base_url := cohort.dataset.web_url():
        # Construct HTML URL viewable in browser
        html_url = str(multiqc_html_path).replace(str(cohort.dataset.web_prefix()), base_url)
        logger.info(f'MultiQC report URL: {html_url}')

    multiqc_data = load_json(
            multiqc_data_path,
            extract_key='report_general_stats_data'
        )
    qc_checker = QCChecker(cohort, multiqc_data, output)

    seq_type = get_config()['workflow']['sequencing_type']

    # Run checks
    bad_lines_by_sample = defaultdict(list)
    check_definitions =  [
        ('min', '<', '≥', lambda val, thresh: val < thresh),
        ('max', '>', '≤', lambda val, thresh: val > thresh),
        ('equality', '!=', '==', lambda val, thresh: val != thresh),
    ]
    for check_type, fail_sign, good_sign, is_fail in check_definitions:
        # 1. Build thresholds for this check type (min, max, or equality)
        threshold_map = build_qc_thresholds(seq_type, check_type, qc_checker)

        # 2. Iterate through MultiQC data sections
        for section_data in qc_checker.multiqc_data.values():
            # 3. Iterate through each sample in the section
            for sg_id, val_by_metric in section_data.items():
                # 4. Iterate through each metric to check
                for metric_config in threshold_map.values():
                    # 5. Get the value for the metric
                    # DRAGEN does not provide pct chimeras directly, so we calculate it
                    # Also, ploidy estimation needs custom calculation
                    (
                        val_to_check,
                        raw_val_for_log,
                        expected_val_for_log,
                    ) = get_metric_value(qc_checker, metric_config, val_by_metric, sg_id)

                    if val_to_check is None:
                        # Metric not found or calculation failed
                        continue

                    threshold = metric_config['threshold']
                    display_name = metric_config['display_name']

                    # 6. Perform the check
                    if is_fail(val_to_check, threshold):
                        # --- FAILURE ---
                        sign = fail_sign
                        is_failure = True
                    else:
                        # --- SUCCESS ---
                        sign = good_sign
                        is_failure = False

                     # 7. Format and log the result
                    line = format_log_line(
                        display_name,
                        val_to_check,
                        threshold,
                        sign,
                        check_type,
                        raw_val_for_log,
                        expected_val_for_log,
                    )

                    if is_failure:
                        logger.warning(f'❗ {sg_id}: {line}')
                        bad_lines_by_sample[sg_id].append(line)
                    else:
                        logger.info(f'✅ {sg_id}: {line}')

    logger.info('') # Newline for readability

    # --- Post-checking steps ---
    if bad_lines_by_sample:
        write_failures_to_json(bad_lines_by_sample, output)
        post_to_slack(bad_lines_by_sample, qc_checker, html_url)
