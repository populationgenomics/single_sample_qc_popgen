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

QC_MAPPING = {
    'mean_coverage': {
        'multiqc_report_name': 'Average sequenced coverage over genome',
        'display_name': 'Mean Coverage',
    },
    'ploidy_estimation': {
        'calculator': (
            lambda d, sg_id, sex_mapping: (
                d['Ploidy estimation'].count('X') == sex_mapping[sg_id],
                sex_mapping[sg_id],
            )
        ),
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
        'calculator': (
            lambda d, _, __: (
                d['Supplementary (chimeric) alignments'] / d['Total alignments'],
                _,
            )
        ),
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

def build_qc_thresholds(seq_type: str, config_key: str) -> dict[str, dict]:
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
        if metric in QC_MAPPING:
            qc_thresholds[metric] = {
                'threshold': threshold,
                **QC_MAPPING[metric],
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


def check_multiqc(
    cohort: Cohort,
    inputs: StageInput,
    outputs: dict[str, str],
):
    from single_sample_qc_popgen.stages import RunMultiQc  # noqa: PLC0415

    seq_type = get_config()['workflow']['sequencing_type']

    cohort_sgs = cohort.get_sequencing_groups()
    reported_sex_mapping_dict: dict[str, int] = get_sgid_reported_sex_mapping(cohort)

    with inputs.as_path(target=cohort, stage=RunMultiQc, key='multiqc_json').open() as f:
        d = json.load(f)
        sections = d['report_general_stats_data']

    bad_lines_by_sample = defaultdict(list)
    for check_type, fail_sign, good_sign, is_fail in [
        ('min', '<', '≥', lambda val, thresh: val < thresh),
        ('max', '>', '≤', lambda val, thresh: val > thresh),
        ('equality', '!=', '==', lambda val, thresh: val != thresh),
    ]:
        threshold_d = build_qc_thresholds(seq_type, check_type)
        for section_data in sections.values():
            for sg_id, val_by_metric in section_data.items():
                for metric_config in threshold_d.values():
                    val = None
                    # DRAGEN does not provide pct chimeras directly, so we calculate it
                    # Also, ploidy estimation needs custom calculation
                    if 'calculator' in metric_config:
                        try:
                            val, expected = metric_config['calculator'](val_by_metric, sg_id, reported_sex_mapping_dict)
                        except (KeyError, ZeroDivisionError):
                            continue
                    elif 'multiqc_report_name' in metric_config:
                        val = val_by_metric.get(metric_config['multiqc_report_name'])

                    if val is None:
                        continue

                    threshold = metric_config['threshold']
                    display_name = metric_config['display_name']

                    if is_fail(val, threshold):
                        if isinstance(val, bool):
                            line = f'{display_name} is {val_by_metric[metric_config["multiqc_report_name"]]} (expected {expected})'
                        else:
                            line = f'{display_name}={val:.4f} {fail_sign} {threshold:.4f}'

                        bad_lines_by_sample[sg_id].append(line)
                        logger.warning(f'❗ {sg_id}: {line}')
                    else:
                        if isinstance(val, bool):
                            line = f'{display_name} is {val_by_metric[metric_config["multiqc_report_name"]]} (expected {expected})'
                        else:
                            line = f'{display_name}={val:.4f} {good_sign} {threshold:.4f}'

                        logger.info(f'✅ {sg_id}: {line}')
    logger.info('')

    if bad_lines_by_sample:
        logger.info(f'Writing {len(bad_lines_by_sample)} failed sample(s) to {outputs["failed_samples"]}')
        with to_path(outputs['failed_samples']).open('w') as f:
            json.dump(bad_lines_by_sample, f, indent=2)

    # Check percent of failed samples in cohort and log warning if >5%
    high_failure_message = None
    if (failure_percent := (len(bad_lines_by_sample) / len(cohort_sgs)) * 100) > 5.0:
        high_failure_message = (
            '=================================\n'
            '🚨 ALERT: High QC Failure Rate 🚨\n'
            '=================================\n'
            f'**Failure Rate:** {len(bad_lines_by_sample)} out of {len(cohort_sgs)} ({failure_percent:.2f}%)'
        )

    # Constructing Slack message
    html_url = str(inputs.as_path(target=cohort, stage=RunMultiQc, key='multiqc_report'))

    title = f'*[{cohort.id}]* <{html_url}|{"MultiQC report"}>'

    messages = []
    if high_failure_message:
        messages.append(high_failure_message)

    if bad_lines_by_sample:
        messages.append(f'{title}. {len(bad_lines_by_sample)} samples are flagged:')
        for sample, bad_lines in bad_lines_by_sample.items():
            messages.append(f'❗ {sample}: ' + ', '.join(bad_lines))
    else:
        messages.append(f'✅ {title}')
    text = '\n'.join(messages)
    logger.info(text)

    # Send to Slack if enabled
    if config_retrieve(['workflow', 'multiqc', 'send_to_slack'], default=True):
        send_message(text)
