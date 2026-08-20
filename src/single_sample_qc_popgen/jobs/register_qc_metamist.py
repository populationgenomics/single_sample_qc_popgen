"""
Register QC metrics from MultiQC into Metamist.
Options to deactivate sequencing groups that failed QC.
"""

import json
from typing import Any

import cpg_utils
from cpg_flow.targets import Cohort, SequencingGroup
from cpg_utils.config import config_retrieve, get_access_level
from loguru import logger
from metamist.graphql import gql, query

from single_sample_qc_popgen.constants import OURDNA_CONTROL
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


CONTROL_SG_QUERY = gql(
    """
    query ControlQuery($cohortId: String!) {
        cohorts(id: {eq: $cohortId}) {
            sequencingGroups {
                id
                sample {
                    externalId
                }
            }
        }
    }
    """
)


def get_control_sg_ids(cohort: Cohort) -> set[str]:
    """Return SG IDs whose sample is an OurDNA control (external ID contains
    OURDNA_CONTROL). Controls are never topped up and must never be deactivated.
    """
    response = query(CONTROL_SG_QUERY, variables={'cohortId': cohort.id})
    control_sg_ids: set[str] = set()
    for coh in response['cohorts']:
        for sg in coh['sequencingGroups']:
            external_id = sg['sample'].get('externalId') or ''
            if OURDNA_CONTROL in external_id:
                control_sg_ids.add(sg['id'])
    return control_sg_ids


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

# Metrics registered to metamist, as (meta_key, general-stats metric name).
METRIC_MAP = [
    # Contamination
    ('contamination_dragen', 'Estimated sample contamination'),

    # Coverage & Yield
    ('mean_coverage', 'Average sequenced coverage over genome'),
    ('median_coverage', 'wgs median autosomal coverage over genome'),
    ('pct_genome_gt_20x', 'wgs pct of genome with coverage [20x:inf)'),
    ('q30_bases_pct', 'Q30 bases pct'),

    # Alignment & Library Quality
    ('mapping_rate_pct', 'Mapped reads pct'),
    ('pct_duplicate_reads', 'Number of duplicate marked reads pct'),
    ('mean_insert_size', 'Insert length: mean'),
    ('std_dev_insert_size', 'Insert length: standard deviation'),
    ('avg_gc_content', 'avg_gc_content_percent'),
    ('chimera_alignments', 'Supplementary (chimeric) alignments'),
    ('total_alignments', 'Total alignments'),

    # Sex & Ploidy
    ('ploidy_estimation', 'Ploidy estimation'),
    ('norm_x_coverage', 'X median / Autosomal median'),
    ('norm_y_coverage', 'Y median / Autosomal median'),

    # Variant QC
    ('ti_tv_ratio', 'Ti/Tv ratio'),
    ('het_hom_ratio', 'Het/Hom ratio'),
]


def resolve_metric_sections(general_stats: dict[str, Any], metric_keys: list[str]) -> dict[str, str]:
    """Maps each metric name to the single general-stats section containing it.

    MultiQC keys report_general_stats_data by module anchor plus an
    auto-incrementing suffix (DRAGEN, DRAGEN_3, ...) whose numbering depends
    on the input file mix, so sections are resolved by content instead of by
    positional key.

    Args:
        general_stats: The report_general_stats_data mapping from multiqc_data.json.
        metric_keys: Metric names to resolve.

    Returns:
        Mapping of metric name to section key.

    Raises:
        ValueError: If any metric appears in zero or in more than one section.
    """
    errors: list[str] = []
    section_by_metric: dict[str, str] = {}
    for metric_key in metric_keys:
        hits = [
            section_key
            for section_key, val_by_metric_by_sample in general_stats.items()
            if any(metric_key in val_by_metric for val_by_metric in val_by_metric_by_sample.values())
        ]
        if len(hits) == 1:
            section_by_metric[metric_key] = hits[0]
        else:
            errors.append(f'{metric_key!r} found in {len(hits)} sections {hits}')
    if errors:
        raise ValueError(
            'Could not resolve MultiQC general-stats sections '
            f'(available sections: {list(general_stats)}): ' + '; '.join(errors)
        )
    return section_by_metric


def build_sg_multiqc_meta_dict(cohort_sgs: list[SequencingGroup], multiqc_json: dict[str, Any]) -> dict[str, dict]:
    """
    Build a dictionary mapping sequencing group IDs to their MultiQC metrics.
    """
    section_by_metric = resolve_metric_sections(multiqc_json, [metric_key for _, metric_key in METRIC_MAP])

    extracted_data = {}

    for sg in cohort_sgs:
        sample_metrics: dict[str, Any] = {}
        missing_sections_for_this_sample = set()

        for out_key, metric_key in METRIC_MAP:
            section_key = section_by_metric[metric_key]
            val_by_metric = multiqc_json[section_key].get(sg.id)

            if val_by_metric is None:
                sample_metrics[out_key] = None
                # Only log if we haven't complained about this specific section for this sample yet
                if section_key not in missing_sections_for_this_sample:
                    logger.warning(f"⚠️ Sequencing Group '{sg.id}' missing from MultiQC section: '{section_key}'")
                    missing_sections_for_this_sample.add(section_key)
                continue

            # Use None if the metric is missing for this sample
            sample_metrics[out_key] = val_by_metric.get(metric_key)

        extracted_data[sg.id] = sample_metrics

    return extracted_data

def update_sg_qc_metrics(
        failed_samples: dict[str, list[str]],
        meta_to_update: dict[str, Any],
        sex_imputation_by_sg: dict[str, dict[str, Any]],
        swap_check_by_sg: dict[str, dict[str, Any]],
        cohort: Cohort,
        output: cpg_utils.Path
    ) -> dict[str, list[str]]:
    cohort_sgs: list[SequencingGroup] = cohort.get_sequencing_groups()
    meta_to_update = build_sg_multiqc_meta_dict(cohort_sgs, meta_to_update)
    if not failed_samples:
        logger.info('No failed samples detected for this cohort QC run.')
    else:
        logger.warning(f'Failed samples: {failed_samples}')
    logger.info(f'meta to update: {meta_to_update}')
    for sg in cohort_sgs:
        sg_meta ={}
        sg_meta['qc'] = meta_to_update.get(sg.id, {})
        # Merge somalier-derived raw signals (f_stat_raw, x_het_rate,
        # n_called_x, y_calls, y_n) alongside MultiQC metrics. Karyotype
        # derivation is performed downstream in ourdna_genomic_atlas.
        sg_meta['qc'].update(sex_imputation_by_sg.get(sg.id, {}))
        # Merge swap-check fields under a nested 'swap_check' key so the
        # status taxonomy is grouped and won't collide with MultiQC metrics.
        # Mapping-layer statuses (e.g. array_pending_export) carry no
        # somalier fields; comparison-layer statuses include best_array_sg
        # / best_relatedness / n_sites_compared. swap_detected is a
        # labelling problem and is intentionally NOT wired into
        # qc_checks_failed or deactivate_sgs.
        if sg.id in swap_check_by_sg:
            sg_meta['qc']['swap_check'] = swap_check_by_sg[sg.id]
        sg_meta['qc']['qc_checks_failed'] = failed_samples.get(sg.id, []) if sg.id in failed_samples else []
        logger.info(f'Updating SG {sg.id} with meta: {sg_meta}')
        metamist_project = cohort.dataset.name
        if get_access_level() == 'test':
            metamist_project += '-test'
        result_update_mutation = query(
            MUTATION_SEQUENCING_GROUP,
            variables={
                'project': metamist_project,
                'sequencingGroup': {
                    'id': sg.id,
                    'meta': sg_meta,
                },
            },
        )
        logger.info(f'Updated SG {sg.id}: {result_update_mutation}')

    # Write out meta fields updated to json
    with output.open('w') as f:
        json.dump(meta_to_update, f, indent=4)

    # Deactivate sequencing groups that failed QC. Controls are excluded:
    # deactivating a control would archive the cohort it belongs to.
    if config_retrieve(['workflow', 'multiqc']).get('deactivate_sgs', False):
        control_sg_ids = get_control_sg_ids(cohort)
        sgs_to_deactivate = [sg for sg in failed_samples if sg not in control_sg_ids]
        skipped_controls = [sg for sg in failed_samples if sg in control_sg_ids]
        if skipped_controls:
            logger.warning(f'Skipping deactivation of failed control samples: {skipped_controls}')
        logger.warning(f'Deactivating failed samples: {sgs_to_deactivate}')
        result_mutation = query(
            MUTATION_DEACTIVATE_SGS,
            variables={'sequencingGroupsToDeactivate': sgs_to_deactivate},
        )['sequencingGroup']['archiveSequencingGroups']
        logger.warning(f'Deactivated sequencing groups: {result_mutation}')

    return failed_samples

def run(
    cohort: Cohort,
    multiqc_data_path: str,
    failures_path: str,
    sex_imputation_path: str,
    swap_check_path: str,
    output: cpg_utils.Path,
):

    multiqc_data = load_json(
        multiqc_data_path,
        extract_key='report_general_stats_data'
    )
    failed_samples = load_json(failures_path, allow_missing=True) or {}
    sex_imputation_by_sg = load_json(sex_imputation_path, allow_missing=True) or {}
    swap_check_by_sg = load_json(swap_check_path, allow_missing=True) or {}

    update_sg_qc_metrics(
        failed_samples=failed_samples,
        meta_to_update=multiqc_data,
        sex_imputation_by_sg=sex_imputation_by_sg,
        swap_check_by_sg=swap_check_by_sg,
        cohort=cohort,
        output=output,
    )
