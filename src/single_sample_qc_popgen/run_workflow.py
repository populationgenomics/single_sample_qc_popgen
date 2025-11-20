#!/usr/bin/env python3

from argparse import ArgumentParser

from cpg_flow.workflow import run_workflow

from single_sample_qc_popgen.stages import CheckMultiQc, RegisterQcMetricsToMetamist, RunMultiQc


def cli_main():
    """
    CLI entrypoint - starts up the workflow
    """
    parser = ArgumentParser()
    parser.add_argument('--dry_run', action='store_true', help='Dry run')
    args = parser.parse_args()

    # Note - in production-pipelines the main.py script sets up layers of default configuration,
    # overlaid with workflow-specific configuration, and then runs the workflow.
    # If you want to re-use that model, this should be carried out before entering the workflow

    # Otherwise all configuration should be done by providing all relevant configs to analysis-runner
    # https://github.com/populationgenomics/team-docs/blob/main/cpg_utils_config.md#config-in-analysis-runner-jobs

    stages = [RunMultiQc, CheckMultiQc, RegisterQcMetricsToMetamist]

    run_workflow(name='single_sample_qc_popgen', stages=stages, dry_run=args.dry_run)


if __name__ == '__main__':
    cli_main()
