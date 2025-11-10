# cpg-flow-pipeline-template
A template repository to use as a base for CPG workflows using the cpg-flow pipeline framework

## Purpose

This pipeline performs sample quality-control (QC) on single-sample sequencing data from the [DRAGEN alignment and genotyping pipeline](https://github.com/populationgenomics/dragen_align_pa). The DRAGEN pipelien produces CRAM files and associated QC metrics, which this pipeline analyses to ensure that the data meets quality standards before being used in downstream analyses.

The metrics used as hard sample filtering are adapted from the All Of Us Research Program (AoURP) QC thresholds. The metrics can be found in the [supplementary materials of the AoURP publication](https://www.nature.com/articles/s41586-023-06957-x#Sec32) (accompanying Word document [here](https://static-content.springer.com/esm/art%3A10.1038%2Fs41586-023-06957-x/MediaObjects/41586_2023_6957_MOESM1_ESM.docx)), as well as below.

The pipeline checks for the `dragen_metrics` QC metrics output of the `dragen_align_pa` pipeline. The contents of this directory are provided to MultiQC to generate a consolidated QC report, which is then parsed to extract the relevant QC metrics for filtering. For samples that fail the hard filters, a message is sent via Slack to notify the relevant team. All pertinent QC metrics are also stored in metamist on a sequencing group level (via the `meta` field). The user can also configure to deactivate the sequencing groups that fail QC, preventing them from being used in downstream analyses.

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

1. **Run MultiQC** on the DRAGEN metrics output for each sample to generate a consolidated QC report and json data file.
2. **Check QC metrics** by parsing the MultiQC json data file to extract relevant QC metrics for each sample. The metrics are compared against predefined thresholds to determine if the sample passes or fails QC. Optionally send Slack notifications for samples that fail QC and deactivate them in metamist.
3. **Register QC metrics in metamist** by storing the extracted QC metrics in the `meta` field of each sequencing group in metamist for future reference.

## Prerequisites

1. **Metamist Cohort**: The pipeline requires a Metamist cohort with sequencing groups corresponding to the samples to be QCed. Ensure that the cohort is properly set up and accessible.
2. **Configuration File**: A configuration file (TOML format) specifying the QC metrics thresholds, Slack notification settings, and other parameters is required. A template configuration file is provided at `config/single_sample_qc_popgen.toml`.


## Configuration

Your TOML configuration file must specify the following key options:

  * `[workflow]`:

      * `input_cohorts`: A list of Metamist cohort IDs to process (e.g., `['COH0001']`).
      * `sequencing_type`: Must be set (e.g., `"genome"`).
      * `last_stages`: A list of the final stages to run. To run the full pipeline use `['RegisterQcMetricsToMetamist']`.
      * `skip_stages`: (Optional) A list of stages to skip, e.g., `['RunMultiQc']`.


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
