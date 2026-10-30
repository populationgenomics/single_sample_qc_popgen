## Purpose

This pipeline performs sample quality-control (QC) on single-sample sequencing data from the [DRAGEN alignment and genotyping pipeline](https://github.com/populationgenomics/dragen_align_pa). The DRAGEN pipelien produces CRAM files and associated QC metrics, which this pipeline analyses to ensure that the data meets quality standards before being used in downstream analyses.

The metrics used for sample hard filters are adapted from the All Of Us Research Program (AoURP) QC thresholds. The metrics can be found in the [supplementary materials of the AoURP publication](https://www.nature.com/articles/s41586-023-06957-x#Sec32) (accompanying Word document [here](https://static-content.springer.com/esm/art%3A10.1038%2Fs41586-023-06957-x/MediaObjects/41586_2023_6957_MOESM1_ESM.docx)), as well as below.

The pipeline reads the `dragen_metrics` output of the `dragen_align_pa` pipeline. An explicit list of per-sequencing-group DRAGEN CSVs (`MULTIQC_BASE_INPUT_SUFFIXES` in `constants.py`, plus four `target_bed_*` files for exomes) is provided to MultiQC to generate a consolidated QC report, which is then parsed to extract the relevant QC metrics for filtering.

Files are listed explicitly rather than discovered by glob because `dragen_metrics/<SG>/` mixes two layout vintages, and the nested duplicates in the older layout overwrite the top-level files when staged into MultiQC's flat input directory (see `docs/dragen-output-schema.md`). A listed file missing from GCS fails the MultiQC job with an error naming the object. The list excludes:

* files MultiQC does not parse (`cnv_metrics`, `sv_metrics`, `roh_metrics`, `wgs_hist`, and the per-region histogram/contig files);
* time metrics CSVs, which record run provenance rather than sample QC. The `<SG>.dragen.time_metrics.csv` file present in newer outputs would also add a spurious `<SG>.dragen` sample to the report.

`RegisterQcMetricsToMetamist` resolves the general-stats sections in `multiqc_data.json` by metric name rather than by the positional `DRAGEN_N` section keys, whose numbering depends on the input file mix. A metric that resolves to zero or to multiple sections fails the job rather than registering wrong values. For samples that fail the hard filters, a message is sent via Slack to notify the relevant team. All pertinent QC metrics are also stored in metamist on a sequencing group level (via the `meta` field). The user can also configure to deactivate the sequencing groups that fail QC, preventing them from being used in downstream analyses.

**Note:** Call rate was only conducted on array data and as such is not included in this pipelines QC filters.
| **QC Check** | **Data Type** | **Pass Threshold** | **Purpose / Error Detected** | **Action / Outcome** |
|---------------|---------------|--------------------|------------------------------|----------------------|
| **Fingerprint concordance** | WGS (compared to array) | Log-likelihood ratio > –3 | Detects sample swaps or major contamination | All WGS–array pairs passed. |
| **Sex concordance** | WGS and arrays | Genetic sex matches self-reported sex at birth, **OR** self-reported sex is “Other” or missing | Detects sample swaps | All samples passed. |
| **Call rate** (not included in this pipeline) | Arrays | > 98% | Detects contamination or lab/prep errors | All array samples passed. |
| **Cross-individual contamination rate** | WGS | < 3% | Detects sample contamination | All WGS samples passed. |
|  | Arrays | Reported only (no filter applied) | Used as a QC metric, not a hard filter | — |
| **Array contamination interaction** | Arrays + WGS | If array contamination > 10%, corresponding WGS sample excluded | Ensures clean sample matching | Contaminated pairs not released. |
| **Coverage** | WGS | ≥ 30× mean coverage<br>≥ 90% of bases ≥20×<br>≥ 8×10¹⁰ aligned Q30 bases<br>≥ 95% of bases ≥20× in 59 Hereditary Disease Risk genes | Detects poor coverage → low sensitivity / precision in variant calling | All WGS samples passed. |


## Pipeline Overview

The workflow performs the following steps:

1. **Run MultiQC: `RunMultiQc`** on the DRAGEN metrics output for each sample to generate a consolidated QC report and json data file.
2. **Check QC metrics: `CheckMultiQc`** by parsing the MultiQC json data file to extract relevant QC metrics for each sample. The metrics are compared against predefined thresholds to determine if the sample passes or fails QC. Optionally send Slack notifications for samples that fail QC and deactivate them in metamist.
  * NOTE: The pipeline preferentially uses the `participant_portal_reported_sex` field from the participant `meta` data to determine sex to sequencing group mapping, falling back to the `reportedSex` field if the preferred field is not available.
3. **Register QC metrics in metamist: `RegisterQcMetricsToMetamist`** by storing the extracted QC metrics in the `meta` field of each sequencing group in metamist for future reference. If there were no failed samples, then `CheckMultiQc` will output an empty JSON file which is handled as such: `RegisterQcMetricsToMetamist` will read the empty JSON file and proceed to only register the QC metrics.

## Prerequisites

1. **Metamist Cohort**: The pipeline requires a Metamist cohort with sequencing groups corresponding to the samples to be QCed. Ensure that the cohort is properly set up and accessible.
2. **Configuration File**: A configuration file (TOML format) specifying the QC metrics thresholds, Slack notification settings, and other parameters is required. A template configuration file is provided at `config/single_sample_qc_popgen.toml`. It is a reference copy, not a default: it is not in the image and is not layered under run configs, so each run config must carry every setting it needs, including `workflow.swap_check.enabled` and a `[qc_thresholds.<sequencing_type>]` block.


## Configuration

Your TOML configuration file must specify the following key options:

  * `[workflow]`:

      * `input_cohorts`: A list of Metamist cohort IDs to process (e.g., `['COH0001']`).
      * `sequencing_type`: Must be set to `"genome"` or `"exome"`; any other value raises. See [Sequencing types](#sequencing-types).
      * `last_stages`: A list of the final stages to run. To run the full pipeline use `['RegisterQcMetricsToMetamist']`.
      * `skip_stages`: (Optional) A list of stages to skip, e.g., `['RunMultiQc']`.

Additional required/optional sections:

```toml
[workflow.swap_check]
enabled = true            # false for cohorts without genotyping arrays. See "Sample-swap check" below

[cramqc]
assume_sorted = true
num_pcs = 4               # Number of principal components (if used in derived metrics)

[workflow.multiqc]
send_to_slack = true      # Send Slack notification summarising failed samples
deactivate_sgs = false    # If true, deactivate sequencing groups that fail any hard QC threshold

[qc_thresholds.genome.min]
mean_coverage = 30
q30_bases = 8e10          # Total aligned Q30 bases threshold

[qc_thresholds.genome.max]
contamination_dragen = 0.03
chimera_rate = 0.03

[qc_thresholds.genome.equality]
ploidy_estimation = true  # Expect ploidy estimation to match reported sex mapping

[ica.pipelines]
dragen_version = 'dragen_3_7_8'  # DRAGEN version tag used for locating metrics
```

Thresholds are read from `[qc_thresholds.<sequencing_type>.*]`; a run whose `qc_thresholds.<sequencing_type>` block is missing, or whose `min`, `max` and `equality` sub-blocks are all missing or empty, raises rather than applying no thresholds (a missing sub-block alongside a non-empty one is fine). A threshold on a metric that is not in that sequencing type's `QC_MAPPING` also raises, because it could never be enforced; the error lists the valid metric names.

Note: Adjust thresholds per dataset QC policy. Setting `send_to_slack = false` suppresses notifications; enabling `deactivate_sgs` removes failing samples from downstream use.

## Sequencing types

The pipeline selects MultiQC inputs and coverage metric names from `workflow.sequencing_type` (`SEQUENCING_TYPE_PROFILES` in `constants.py`). Only `"genome"` and `"exome"` are supported; any other value raises rather than falling back to genome.

| | `genome` | `exome` |
|---|---|---|
| MultiQC inputs | `MULTIQC_BASE_INPUT_SUFFIXES` | base list plus `target_bed_{coverage_metrics,overall_mean_cov,contig_mean_cov,fine_hist}.csv` |
| Mean coverage (`mean_coverage`) | `Average sequenced coverage over genome` | `Average sequenced coverage over target region` |
| Median coverage (`median_coverage`) | `wgs median autosomal coverage over genome` | `target bed median autosomal coverage over target region` |
| Fraction at ≥20x coverage | `pct_genome_gt_20x` from `wgs pct of genome with coverage [20x:inf)` | `pct_target_gt_20x` from `target bed pct of target region with coverage [20x:inf)` |

`mean_coverage` and `median_coverage` keep their names for both types in the metamist `qc` meta and in `qc_thresholds`, and both can be thresholded (the default genome block sets neither). The ≥20x metric is named for its denominator, so an exome threshold is configured as `[qc_thresholds.exome.min] pct_target_gt_20x = ...`. The metric names are those MultiQC 1.30 (the pipeline's image version) emits for DRAGEN exome metrics.

**Default exome thresholds.** `config/single_sample_qc_popgen.toml` carries an exome block alongside the genome one: `median_coverage >= 50` (on-target median autosomal coverage), `mapping_rate_pct >= 80`, `q30_bases_pct >= 80` and `chimera_rate <= 0.03`. `pct_target_gt_20x`, `contamination_dragen` and `ploidy_estimation` are deliberately not set for exomes yet; they wait on a calibration pass across both mackenzie capture designs (twist-vcgs, CREv2) and sequencing sites. A run config can override any of these or add the deferred ones. Metric names must be in the sequencing type's `QC_MAPPING`: a genome-named key such as `pct_genome_gt_20x` in an exome block raises, and so does any other unmapped key.

## Sample-swap check

Five stages (`PrepareSampleSwap`, `SwapCheckExportVcf`, `SwapCheckSomalierExtract`, `SwapCheckSomalierRelate`, `SwapCheckClassify`) compare each WGS somalier sketch against the sketches of a genotyping-array cohort to detect sample swaps. Their result is registered under `sg.meta['qc']['swap_check']` by `RegisterQcMetricsToMetamist`. This needs `workflow.swap_check.rolling_aggregate_cohort_id` and an `array_aggregate_pgen` analysis registered against that cohort.

`workflow.swap_check.enabled` switches the check. It has no default: every run config must set it, and the pipeline errors on import if it is missing. Cohorts with arrays set `enabled = true`; cohorts without arrays set `enabled = false`. `RegisterQcMetricsToMetamist` then no longer requires `SwapCheckClassify`, so none of the five stages are in the workflow, and `swap_check` is deleted from `sg.meta['qc']`, including any result from an earlier run with `enabled = true`. With `enabled = true` behaviour is unchanged.

## Output versions and reuse

Each stage writes under `.../<stage>/<cohort>/<version>/`, and cpg-flow reuses a stage whenever the files at that path exist. It does not check whether the stage's inputs or code changed. So when a change gives a stage new inputs or changes what it writes, bump that stage's entry in `STAGE_DEFAULT_VERSIONS` (`stages.py`). Otherwise cohorts that already ran it silently keep the old result.

`RegisterQcMetricsToMetamist` was bumped to `v2` for this reason. OurDNA cohorts registered before the swap check worked kept their `v1/{cohort}_registered.json`. Later runs produced swap-check results but reused that file, so `swap_check` never reached metamist. That affected 13 cohorts and 846 of the 1254 `COH15110` SGs; for example, COH13800 was registered on 2026-06-29 and its swap check ran on 2026-08-18.

`RunMultiQc` and `CheckMultiQc` were bumped to `v2` with it. `v1` exome MultiQC JSONs were built without the `target_bed_*` inputs, so they lack the target-region coverage metrics and registering them as exome raises. `v1` failures were computed before thresholds were applied per sequencing type, and some are empty: tenk10k-phase2 genomes run as `exome` had no thresholds applied.

All three now default to `v3`, because `v2` outputs already existed for OurDNA COH10475, written on 2026-04-22 by an earlier run. Under a `v2` default, cpg-flow would have reused them, so COH10475 would not have been re-QC'd or re-registered. No cohort had `v3` outputs, so on its next run every cohort re-runs MultiQC, the threshold checks and registration once, reusing its swap-check outputs. Its MultiQC report moves to a `v3/` web URL, and its Slack QC summary is re-sent unless `workflow.send_to_slack = false`.

Before bumping a version, check that no cohort already has outputs at the new version, for example: `gcloud storage ls 'gs://cpg-<dataset>-main/single_sample_qc_popgen/<stage>/*/<version>/'` (for exomes, under `gs://cpg-<dataset>-main/exome/`).

Version precedence: `workflow.output_versions.<stage>`, then `workflow.version`, then `STAGE_DEFAULT_VERSIONS`, then `v1`.

## How to Run the Pipeline

The pipeline is launched using `analysis-runner`.

**Example Invocation:**

```bash
analysis-runner \
--dataset <your-dataset> \
--access test \
--config <path/to/your-config.toml> \
--output-dir '' \
--description "DRAGEN QC checks for <your-cohort>" \
--image "australia-southeast1-docker.pkg.dev/cpg-common/images-dev/single_sample_qc_popgen:<image-tag>" \
single_sample_qc_popgen
```

  * `--dataset`: The Metamist dataset associated with your cohort.
  * `--config`: The path to your local TOML configuration file.
  * `--output-dir`: This is required by `analysis-runner` but is not used by this pipeline. You can leave it as `''`.
  * `--image`: The full path to the pipeline's Docker image. The example uses a `-dev` image, but production runs will use a production (i.e. no `-dev` image)


## Pipeline Outputs
When successful the pipeline will produce the following outputs:
  * **RunMultiQc**
      * `<cohort_id>_multiqc_report.html`: The MultiQC HTML report for the cohort.
      * `<cohort_id>_multiqc_data.json`: The MultiQC JSON data file containing consolidated QC metrics.
  * **CheckQcMetrics**
      * `<cohort_id>_failed_samples.json`: A JSON file listing samples that failed QC checks along with the reasons for failure.
        * Could be an empty JSON file if no samples failed QC.
  * **RegisterQcMetricsToMetamist**
      * `<cohort_id>_registered.json`: A JSON file summarising the QC metrics that were registered to metamist for each sequencing group.
