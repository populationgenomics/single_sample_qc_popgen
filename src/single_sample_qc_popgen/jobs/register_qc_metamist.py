"""
Register QC metrics from MultiQC into Metamist.
Options to deactivate sequencing groups that failed QC.
"""

import json
from typing import Any

import cpg_utils
from cpg_flow.targets import Cohort, SequencingGroup
from cpg_utils.config import config_retrieve
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

def build_sg_multiqc_meta_dict(multiqc_json: dict[str, Any]) -> dict[str, dict]:
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
        ('pct_genome_20x', 'DRAGEN_5', 'wgs pct of genome with coverage [20x:inf)'),
        ('pct_q30_bases', 'DRAGEN', 'Q30 bases pct'),

        # Alignment & Library Quality
        ('pct_mapped_reads', 'DRAGEN', 'Mapped reads pct'),
        ('pct_duplicate_reads', 'DRAGEN', 'Number of duplicate marked reads pct'),
        ('mean_insert_size', 'DRAGEN', 'Insert length: mean'),
        ('std_dev_insert_size', 'DRAGEN', 'Insert length: standard deviation'),
        ('avg_gc_content', 'dragen-fastqc', 'avg_gc_content_percent'),

        # Sex & Ploidy
        ('ploidy_estimation', 'DRAGEN_4', 'Ploidy estimation'),
        ('norm_x_coverage', 'DRAGEN_4', 'X median / Autosomal median'),
        ('norm_y_coverage', 'DRAGEN_4', 'Y median / Autosomal median'),

        # Variant QC
        ('ti_tv_ratio', 'DRAGEN_3', 'Ti/Tv ratio'),
        ('het_hom_ratio', 'DRAGEN_3', 'Het/Hom ratio'),
    ]

    extracted_data = {}
    # Get a list of all CPG IDs from one of the tools
    sample_ids = list(multiqc_json.get('DRAGEN', {}).keys())

    if not sample_ids:
        logger.error("Error: Could not find any sample IDs in the data.")

    for cpg_id in sample_ids:
        sample_metrics = {}
        for out_key, tool_key, metric_key in metric_map:
            try:
                value = multiqc_json[tool_key][cpg_id][metric_key]
                sample_metrics[out_key] = value
            except (KeyError, TypeError):
                # Use None if the metric is missing for this sample
                sample_metrics[out_key] = None

        extracted_data[cpg_id] = sample_metrics

    return extracted_data

def update_sg_qc_metrics(
        failed_samples: dict[str, list[str]],
        meta_to_update: dict[str, Any],
        cohort: Cohort,
        output: cpg_utils.Path
    ) -> dict[str, list[str]]:
    cohort_sgs: list[SequencingGroup] = cohort.get_sequencing_groups()
    meta_to_update = build_sg_multiqc_meta_dict(meta_to_update)
    logger.warning(f'Failed samples: {failed_samples}')
    logger.info(f'meta to update: {meta_to_update}')
    for sg in cohort_sgs:
        sg_meta ={}
        sg_meta['qc'] = meta_to_update.get(sg.id, {})
        sg_meta['qc']['qc_checks_failed'] = failed_samples.get(sg.id, []) if sg.id in failed_samples else []
        logger.info(f'Updating SG {sg.id} with meta: {sg_meta}')
        result_update_mutation = query(
            MUTATION_SEQUENCING_GROUP,
            variables={
                'project': f'{cohort.dataset.name}-test',
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
    failed_samples_path: str,
    output: cpg_utils.Path,
):

    multiqc_data = load_json(
        multiqc_data_path,
        extract_key='report_general_stats_data'
    )
    failed_samples = load_json(
        failed_samples_path,
        allow_missing=True,
    )

    update_sg_qc_metrics(
        failed_samples=failed_samples,
        meta_to_update=multiqc_data,
        cohort=cohort,
        output=output,
    )
