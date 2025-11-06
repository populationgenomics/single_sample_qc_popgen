#!/usr/bin/env python3

"""
Batch jobs to run MultiQC.
"""
import json
from typing import cast

from cpg_flow.resources import STANDARD
from cpg_flow.targets import Cohort, SequencingGroup
from cpg_flow.utils import rich_sequencing_group_id_seds
from cpg_utils import Path, to_path
from cpg_utils.config import config_retrieve, get_config, image_path
from cpg_utils.hail_batch import command, copy_common_env
from hailtop.batch import Batch, ResourceFile
from hailtop.batch.job import Job, PythonJob
from loguru import logger
from metamist.graphql import gql, query

from single_sample_qc_popgen.jobs import check_multiqc
from single_sample_qc_popgen.utils import get_output_path

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


def get_sgid_reported_sex_mapping(cohort: Cohort) -> dict[str, str]:
    """
    Get a mapping of sequencing group ID to reported sex.
    """
    mapping: dict[str, int] = {}
    response = query(REPORTED_SEX_QUERY, variables={'cohortId': cohort.id})
    for coh in response['cohorts']:
        for sg in coh   ['sequencingGroups']:
            mapping[sg['id']] = sg['sample']['participant']['reportedSex']
    return mapping

def build_sg_multiqc_meta_dict(multiqc_json: ResourceFile) -> dict[str, dict]:
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

    with open(multiqc_json) as f:
        multiqc_json = json.load(f)
        multiqc_json = multiqc_json['report_general_stats_data']

    extracted_data = {}
    # Get a list of all CPG IDs from one of the tools
    sample_ids = list(multiqc_json.get('verifybamid', {}).keys())
    if not sample_ids:
        # Fallback if 'verifybamid' is missing
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

def update_sg_qc_metrics(failed_meta: ResourceFile | None, meta_to_update: ResourceFile, cohort: Cohort):
    cohort_sgs: list[SequencingGroup] = cohort.get_sequencing_groups()
    meta_to_update = build_sg_multiqc_meta_dict(meta_to_update)
    # check_j.output (failed_meta) may not exist if qc_thresholds not set in config
    try:
        with open(failed_meta) as fh:
            failed_samples: dict[str, list[str]] = json.load(fh)
        logger.warning(f'Failed samples: {failed_samples}')
        logger.warning(f'meta to update: {meta_to_update}')
        for sg in cohort_sgs:
            sg_meta ={}
            sg_meta['qc'] = meta_to_update.get(sg.id, {})
            sg_meta['qc']['qc_checks_failed'] = failed_samples.get(sg.id, []) if sg.id in failed_samples else []
            logger.warning(f'Updating SG {sg.id} with meta: {sg_meta}')
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
            logger.warning(f'Updated SG {sg.id}: {result_update_mutation}')

        # Deactivate sequencing groups that failed QC
        if get_config()['workflow']['multiqc'].get('deactivate_sgs', False):
            logger.warning(f'Deactivating failed samples: {list(failed_samples.keys())}')
            result_mutation = query(
                MUTATION_DEACTIVATE_SGS,
                variables={'sequencingGroupsToDeactivate': list(failed_samples.keys())},
            )['sequencingGroup']['archiveSequencingGroups']
            logger.warning(f'Deactivated sequencing groups: {result_mutation}')

    except json.JSONDecodeError:
        logger.error(f'Failed to decode JSON from {failed_meta}. No failed samples registered.')

    return failed_samples

def multiqc(
    b: Batch,
    cohort: Cohort,
    tmp_prefix: Path,
    paths: list[Path],
    out_json_path: Path,
    out_html_path: Path,
    out_html_url: str | None = None,
    out_checks_path: Path | None = None,
    label: str | None = None,
    ending_to_trim: set[str] | None = None,
    modules_to_trim_endings: set[str] | None = None,
    job_attrs: dict | None = None,
    sequencing_group_id_map: dict[str, str] | None = None,
    send_to_slack: bool = True,
    extra_config: dict | None = None,
) -> list[Job]:
    """
    Run MultiQC for the files in `qc_paths`
    @param b: batch object
    @param tmp_prefix: bucket for tmp files
    @param paths: file bucket paths to pass into MultiQC
    @param cohort: Cohort object
    @param out_json_path: where to write MultiQC-generated JSON file
    @param out_html_path: where to write the HTML report
    @param out_html_url: URL corresponding to the HTML report
    @param out_checks_path: flag indicating that QC checks were done
    @param label: To add to the report's, Batch job's, and Slack message's titles
    @param ending_to_trim: trim these endings from input files to get sequencing group names
    @param modules_to_trim_endings: list of modules for which trim the endings
    @param job_attrs: attributes to add to Hail Batch job
    @param sequencing_group_id_map: sequencing group ID map for bulk sequencing group renaming:
        (https://multiqc.info/docs/#bulk-sample-renaming-in-reports)
    @param send_to_slack: whether or not to send a Slack message to the qc channel
    @param extra_config: extra config to pass to MultiQC
    @return: job objects
    """
    title = 'MultiQC'
    if label:
        title += f' [{label}]'


    mqc_j = b.new_job(title, (job_attrs or {}) | dict(tool='MultiQC'))
    mqc_j.image(image_path('multiqc', '1.30-3'))
    STANDARD.set_resources(j=mqc_j, ncpu=16)

    file_list_path = tmp_prefix / f'{cohort.get_alignment_inputs_hash()}_multiqc-file-list.txt'
    if not get_config()['workflow'].get('dry_run', False):
        with file_list_path.open('w') as f:
            f.writelines([f'{p}\n' for p in paths])
    file_list = b.read_input(str(file_list_path))

    endings_conf = ', '.join(list(ending_to_trim)) if ending_to_trim else ''
    modules_conf = ', '.join(list(modules_to_trim_endings)) if modules_to_trim_endings else ''

    if sequencing_group_id_map:
        sample_map_path = tmp_prefix / f'{cohort.get_alignment_inputs_hash()}_rename-sample-map.tsv'
        if not get_config()['workflow'].get('dry_run', False):
            _write_sg_id_map(sequencing_group_id_map, sample_map_path)
        sample_map_file = b.read_input(str(sample_map_path))
    else:
        sample_map_file = None

    if extra_config:
        extra_config_param = ''
        for k, v in extra_config.items():
            serialised = f'{k}: {v}'
            extra_config_param += f"""--cl-config "{serialised}" \\
            """
    else:
        extra_config_param = ''

    report_filename = 'report'
    cmd = f"""\
    mkdir inputs
    cat {file_list} | gsutil -m cp -r -I inputs/

    multiqc -f inputs -o output \\
    {f"--replace-names {sample_map_file} " if sample_map_file else ''} \\
    --title "{title} for dataset <b>{cohort.dataset.name}</b>" \\
    --filename {report_filename}.html \\
    --cl-config "extra_fn_clean_exts: [{endings_conf}]" \\
    --cl-config "max_table_rows: 10000" \\
    --cl-config "use_filename_as_sample_name: [{modules_conf}]" \\
    {extra_config_param}

    ls output/{report_filename}_data
    cp output/{report_filename}.html {mqc_j.html}
    cp output/{report_filename}_data/multiqc_data.json {mqc_j.json}
    """
    if out_html_url:
        cmd += '\n' + f'echo "HTML URL: {out_html_url}"'

    mqc_j.command(command(cmd, setup_gcp=True))
    b.write_output(mqc_j.html, str(out_html_path))
    b.write_output(mqc_j.json, str(out_json_path))

    assert isinstance(mqc_j.json, ResourceFile)
    jobs: list[Job] = [mqc_j]
    check_j: Job | None = None
    if get_config().get('qc_thresholds'):
        sg_reported_sex_mapping: dict[str, str] = get_sgid_reported_sex_mapping(cohort)
        check_j = check_report_job(
            b=b,
            multiqc_json_file=mqc_j.json,
            multiqc_html_url=out_html_url,
            rich_id_map=cohort.dataset.rich_id_map(),
            cohort_id=cohort.id,
            num_sgs=len(cohort.get_sequencing_groups()),
            reported_sex_mapping=sg_reported_sex_mapping,
            label=label,
            out_checks_path=out_checks_path,
            job_attrs=job_attrs,
            send_to_slack=send_to_slack,
        )
        check_j.depends_on(mqc_j)
        jobs.append(check_j)

    register_qc_j: PythonJob = b.new_python_job('Register MultiQC results in Metamist')
    register_qc_j.image(config_retrieve(['workflow', 'driver_image']))
    register_qc_j.call(
        update_sg_qc_metrics,
        check_j.output if check_j else None,
        mqc_j.json,
        cohort,
    )
    if check_j:
        register_qc_j.depends_on(mqc_j, check_j)
    else:
        register_qc_j.depends_on(mqc_j)
    jobs.append(register_qc_j)

    return jobs


def check_report_job(
    b: Batch,
    multiqc_json_file: ResourceFile,
    cohort_id: str,
    num_sgs: int,
    multiqc_html_url: str | None = None,
    reported_sex_mapping: dict[str, str] | None = None,
    label: str | None = None,
    rich_id_map: dict[str, str] | None = None,
    out_checks_path: Path | None = None,
    job_attrs: dict | None = None,
    send_to_slack: bool = True,
) -> Job:
    """
    Run job that checks MultiQC JSON result and sends a Slack notification
    about failed samples.
    """
    title = 'MultiQC'
    if label:
        title += f' [{label}]'
    check_j = b.new_job(f'{title} check', (job_attrs or {}) | dict(tool='python'))
    STANDARD.set_resources(j=check_j, ncpu=2)
    check_j.image(config_retrieve(['workflow', 'driver_image']))

    script_path = to_path(check_multiqc.__file__)
    script_name = script_path.name
    cmd = f"""\
    {rich_sequencing_group_id_seds(rich_id_map, [multiqc_json_file])
    if rich_id_map else ''}

    python3 {script_name} \\
    --multiqc-json {multiqc_json_file} \\
    --html-url {multiqc_html_url} \\
    --cohort-id {cohort_id} \\
    --title "{title}" \\
    --{"no-" if not send_to_slack else ""}send-to-slack \\
    --failed-samples-path {check_j.output} \\
    --reported-sex-mapping '{json.dumps(reported_sex_mapping)}' \\
    --num-sgs {num_sgs}

    echo "HTML URL: {multiqc_html_url}"
    """

    copy_common_env(cast('Job', check_j))
    check_j.command(
        command(
            cmd,
            python_script_path=script_path,
            setup_gcp=True,
        ),
    )
    if out_checks_path:
        b.write_output(check_j.output, str(out_checks_path))
    return check_j


def _write_sg_id_map(sequencing_group_map: dict[str, str], out_path: Path):
    """
    Configuring MultiQC to support bulk sequencing group rename. `sequencing_group_map` is a dictionary
    of sequencing group IDs. The map doesn't have to have records for all sequencing groups.
    Example:
    {
        'SID1': 'Patient1',
        'SID2': 'Patient2'
    }
    https://multiqc.info/docs/#bulk-sample-renaming-in-reports
    """
    with out_path.open('w') as fh:
        for sgid, new_sgid in sequencing_group_map.items():
            fh.write('\t'.join([sgid, new_sgid]) + '\n')



# -------------- Alex's Code ---------------- #
from cpg_flow.targets import Cohort
from cpg_utils import Path  # pyright: ignore[reportUnknownVariableType]
from cpg_utils.config import get_driver_image
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import BashJob


def run_multiqc(
    cohort: Cohort,
    outputs: dict[str, str],
) -> BashJob | None:
    """
    Creates and calls the Job to run MultiQC.
    Gathers all required QC input paths.
    """

    dragen_metric_prefixes: list[Path] = []
    somalier_paths: list[Path] = []
    for sg in cohort.get_sequencing_groups():
        # 1. Get Dragen metric directory prefixes for each SG
        dragen_prefix = get_output_path(filename=f'dragen_metrics/{sg.name}')
        dragen_metric_prefixes.append(dragen_prefix)

        # 2. Get Somalier paths for each SG
        somalier_path = get_output_path(filename=f'{sg.id}.somalier')
        somalier_paths.append(somalier_path)

    # 3. Collect all individual Dragen CSV file paths
    all_dragen_csv_paths: list[Path] = []
    for prefix in dragen_metric_prefixes:
        try:
            # Use rglob to find all CSV files recursively within the SG's metric directory
            found_paths = [to_path(p) for p in prefix.rglob('*.csv')]
            all_dragen_csv_paths.extend(found_paths)
        except FileNotFoundError:
            logger.warning(f'Directory {prefix} not found when searching for Dragen CSVs.')
        except Exception as e:  # noqa: BLE001
            logger.error(f'Error searching for CSVs in {prefix}: {e}')

    # 4. Combine Dragen CSV paths and Somalier paths
    all_qc_paths: list[Path] = all_dragen_csv_paths + somalier_paths

    if not all_qc_paths:
        logger.warning('No QC files (Dragen CSVs or Somalier) found to aggregate with MultiQC')
        return None  # Return None to signal the stage to skip

    logger.info(f'Found {len(all_qc_paths)} QC files for MultiQC aggregation.')
    if all_qc_paths:
        logger.info(f'Example QC paths: {all_qc_paths[:5]}')

    # 5. Create the Job
    b = get_batch()
    multiqc_job: BashJob = b.new_job(
        name='MultiQC',
        attributes=(cohort.get_job_attrs() or {}) | {'tool': 'MultiQC'},  # pyright: ignore[reportUnknownArgumentType]
    )
    multiqc_job.image(image=get_driver_image())
    multiqc_job.storage('10Gi')

    # Read all QC files into the job's input directory
    input_file_dict: dict[str, str] = {f'file_{i}': str(p) for i, p in enumerate(all_qc_paths)}
    b_input_dir_resource = b.read_input_group(**input_file_dict)

    report_name = f'{cohort.name}_multiqc_report'
    multiqc_job.declare_resource_group(
        out={
            'html': f'{report_name}.html',
            'json': f'{report_name}_data/multiqc_data.json',
        }
    )

    # Define the command
    multiqc_job.command(
        f"""
        multiqc \\
        {b_input_dir_resource} \\
        -o {multiqc_job.outdir} \\
        --title 'MultiQC Report for {cohort.name}' \\
        --filename '{report_name}.html' \\
        --cl-config 'max_table_rows: 10000'

        mv {multiqc_job.outdir}/{report_name}.html {multiqc_job.html}
        mv {multiqc_job.outdir}/{report_name}_data/multiqc_data.json {multiqc_job.json}
        """
    )

    # Write outputs to their final GCS locations
    b.write_output(multiqc_job.html, outputs['multiqc_report'])
    b.write_output(multiqc_job.json, outputs['multiqc_data'])

    return multiqc_job
