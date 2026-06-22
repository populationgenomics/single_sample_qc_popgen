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

def build_sg_multiqc_meta_dict(cohort_sgs: list[SequencingGroup], multiqc_json: dict[str, Any]) -> dict[str, dict]:
    """
    Build a dictionary mapping sequencing group IDs to their MultiQC metrics.
    """
    metric_map = [
        # Contamination
        ('freemix', 'verifybamid', 'FREEMIX'),
        ('contamination_dragen', 'DRAGEN', 'Estimated sample contamination'),

        # Coverage & Yield
        ('mean_coverage', 'DRAGEN', 'Average sequenced coverage over genome'),
        ('median_coverage', 'DRAGEN_5', 'wgs median autosomal coverage over genome'),
        ('pct_genome_gt_20x', 'DRAGEN_5', 'wgs pct of genome with coverage [20x:inf)'),
        ('q30_bases_pct', 'DRAGEN', 'Q30 bases pct'),

        # Alignment & Library Quality
        ('mapping_rate_pct', 'DRAGEN', 'Mapped reads pct'),
        ('pct_duplicate_reads', 'DRAGEN', 'Number of duplicate marked reads pct'),
        ('mean_insert_size', 'DRAGEN', 'Insert length: mean'),
        ('std_dev_insert_size', 'DRAGEN', 'Insert length: standard deviation'),
        ('avg_gc_content', 'dragen-fastqc', 'avg_gc_content_percent'),
        ('chimera_alignments', 'DRAGEN', 'Supplementary (chimeric) alignments'),
        ('total_alignments', 'DRAGEN', 'Total alignments'),

        # Sex & Ploidy
        ('ploidy_estimation', 'DRAGEN_4', 'Ploidy estimation'),
        ('norm_x_coverage', 'DRAGEN_4', 'X median / Autosomal median'),
        ('norm_y_coverage', 'DRAGEN_4', 'Y median / Autosomal median'),

        # Variant QC
        ('ti_tv_ratio', 'DRAGEN_3', 'Ti/Tv ratio'),
        ('het_hom_ratio', 'DRAGEN_3', 'Het/Hom ratio'),
    ]

    extracted_data = {}

    for sg in cohort_sgs:
        sample_metrics: dict[str, Any] = {}
        missing_tools_for_this_sample = set()

        for out_key, tool_key, metric_key in metric_map:
            # Check tool key exists
            if tool_key not in multiqc_json:
                sample_metrics[out_key] = None
                continue

            # Extract metric values for sequencing group, handle missing keys
            if sg.id not in multiqc_json[tool_key]:
                sample_metrics[out_key] = None
                # Only log if we haven't complained about this specific tool for this sample yet
                if tool_key not in missing_tools_for_this_sample:
                    logger.warning(f"⚠️ Sequencing Group '{sg.id}' missing from MultiQC module: '{tool_key}'")
                    missing_tools_for_this_sample.add(tool_key)
                continue

            try:
                value = multiqc_json[tool_key][sg.id][metric_key]
                sample_metrics[out_key] = value
            except (KeyError, TypeError):
                # Use None if the metric is missing for this sample
                sample_metrics[out_key] = None

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

    # Deactivate sequencing groups that failed QC
    if config_retrieve(['workflow', 'multiqc']).get('deactivate_sgs', False):
        logger.warning(f'Deactivating failed samples: {list(failed_samples.keys())}')
        result_mutation = query(
            MUTATION_DEACTIVATE_SGS,
            variables={'sequencingGroupsToDeactivate': list(failed_samples.keys())},
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
