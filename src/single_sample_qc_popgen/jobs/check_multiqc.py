#!/usr/bin/env python3

"""
Checks metrics in MultiQC output, based on thresholds in the qc_thresholds
config section.

Script can send a report to a Slack channel. To enable that, set SLACK_TOKEN
and SLACK_CHANNEL environment variables, and add "Seqr Loader" app into
a channel with:

/invite @Seqr Loader
"""
import json
from collections import defaultdict
from typing import Any

from cpg_flow.stage import StageInput
from cpg_flow.targets import Cohort
from cpg_utils import to_path
from cpg_utils.config import config_retrieve, get_config
from cpg_utils.slack import send_message
from loguru import logger
from metamist.graphql import gql, query

REPORTED_SEX_QUERY = gql(
    """
    query MyQuery($cohortId: String!) {
        cohorts(id: {eq: $cohortId}) {
            sequencingGroups {
            id
            sample {
                participant {
                    reportedSex
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
    """
    mapping: dict[str, int] = {}
    response = query(REPORTED_SEX_QUERY, variables={'cohortId': cohort.id})
    for coh in response['cohorts']:
        for sg in coh   ['sequencingGroups']:
            mapping[sg['id']] = sg['sample']['participant']['reportedSex']
    return mapping


class QCChecker:
    """
    Encapsulates all logic for checking a MultiQC report for a cohort.
    """

    def __init__(self, cohort: Cohort, inputs: StageInput, outputs: dict[str, str]):
        self.cohort = cohort
        self.inputs = inputs
        self.outputs = outputs
        # Data to be loaded
        self.cohort_sgs = self.cohort.get_sequencing_groups()
        self.sex_mapping = get_sgid_reported_sex_mapping(self.cohort)
        self.multiqc_data = load_multiqc_data(cohort, inputs)
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

    def _calculate_ploidy(self, d: dict, sg_id: str, sex_mapping: dict[str, int])-> tuple[bool | None, str, str]:
        """
        Calculator for ploidy.
        Returns: (value_to_check, raw_value_for_log, expected_value_for_log)
        """
        raw_ploidy = d.get('Ploidy estimation', 'Unknown')
        expected_sex_num = sex_mapping.get(sg_id)

        if expected_sex_num is None:
            # Cannot check, but can log
            return None, raw_ploidy, f"Unknown (no sex for {sg_id})"
        if raw_ploidy == 'Unknown':
            # Cannot check
            return None, raw_ploidy, str(expected_sex_num)


        is_match = raw_ploidy.count('X') == expected_sex_num

        # Convert expected_sex_num to XX/XY
        expected_ploidy = 'XX' if expected_sex_num == 2 else 'XY'

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
        q30_bases = 8e10
        [qc_thresholds.genome.max]
        contamination_verifybamid = 0.05
        contamination_dragen = 0.03
        chimera_rate = 0.03
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
            )
            qc_thresholds[metric] = {
                'threshold': threshold,
                'multiqc_report_name': metric,
                'display_name': metric,
            }
    return qc_thresholds

def load_multiqc_data(cohort: Cohort, inputs: StageInput) -> dict[str, Any]:
    """Loads the MultiQC JSON data from the input stage."""
    from single_sample_qc_popgen.stages import RunMultiQc  # noqa: PLC0415

    json_path = inputs.as_path(
        target=cohort, stage=RunMultiQc, key='multiqc_json'
    )
    logger.info(f"Loading MultiQC data from: {json_path}")
    with json_path.open() as f:
        data: dict[str, Any] = json.load(f)
        if not isinstance(data, dict) or data is None:
            raise ValueError("Invalid MultiQC data format.")
        return data.get('report_general_stats_data', {})

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

def write_failures_to_json(bad_lines_by_sample: dict[str, list[str]], outputs: dict[str, str]) -> None:
        """Writes all failed sample logs to a JSON file."""
        output_path = outputs['failed_samples']
        logger.warning(
            f'Writing {len(bad_lines_by_sample)} failed sample(s) to {output_path}'
        )
        with to_path(output_path).open('w') as f:
                json.dump(bad_lines_by_sample, f, indent=4)

def post_to_slack(bad_lines_by_sample: dict[str, list[str]], qc_checker: QCChecker, inputs: StageInput) -> None:
    """Constructs and sends the final Slack message."""
    from single_sample_qc_popgen.stages import RunMultiQc  # noqa: PLC0415

    num_failed = len(bad_lines_by_sample)
    num_total_sgs = len(qc_checker.cohort_sgs)

    # 1. Check for high failure rate
    high_failure_message = None
    if num_total_sgs > 0 and (num_failed / num_total_sgs) > 0.05:
        failure_percent = (num_failed / num_total_sgs) * 100
        high_failure_message = (
            '=================================\n'
            '🚨 ALERT: High QC Failure Rate 🚨\n'
            '=================================\n'
            f'**Failure Rate:** {num_failed} out of {num_total_sgs} ({failure_percent:.2f}%)'
        )

    # 2. Construct the main message
    html_path = str(inputs.as_path(
        target=qc_checker.cohort, stage=RunMultiQc, key='multiqc_report'
    ))

    title = f'*[{qc_checker.cohort.id}]* <{html_path}|{"MultiQC report"}>'
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
    inputs: StageInput,
    outputs: dict[str, str],
):

    qc_checker = QCChecker(cohort, inputs, outputs)

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
                        log_func = logger.warning
                        log_symbol = '❗'
                    else:
                        # --- SUCCESS ---
                        sign = good_sign
                        is_failure = False
                        log_func = logger.info
                        log_symbol = '✅'

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

                        log_func(f'{log_symbol} {sg_id}: {line}')

                        if is_failure:
                            logger.warning(f'❗ {sg_id}: {line}')
                            bad_lines_by_sample[sg_id].append(line)
                        else:
                            logger.info(f'✅ {sg_id}: {line}')

    logger.info('') # Newline for readability

    # --- Post-checking steps ---
    if bad_lines_by_sample:
        write_failures_to_json(bad_lines_by_sample, outputs)
        post_to_slack(bad_lines_by_sample, qc_checker, inputs)

