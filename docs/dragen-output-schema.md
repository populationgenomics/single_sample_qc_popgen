# ICA DRAGEN metrics output schema: 2025 vs. 2026 vintage

`gs://cpg-ourdna-main/ica/dragen_3_7_8/output/dragen_metrics/<SG>/` contains two
structurally different output layouts, seemingly depending on when a sequencing group's
ICA DRAGEN analysis ran.

This matters because `jobs/run_multiqc.py` discovers each sequencing group's
CSVs with a recursive glob (`dragen_prefix.rglob('*.csv')`) and stages them
into one shared directory via `gcloud storage cp -I <dir>`, which copies by
basename only. OurDNA Filipino sequencing groups have duplicate/generic-named
files nested under `supplemental/` and `sv/` that collide with each other (or
with the top-level copy) at that shared destination path. Due to the staging,
only the last-copied file survives to be parsed by MultiQC. Rows marked below
as "collides" are affected; see the "Collision impact" note at the end.

## OurDNA Filipino pilot layout

```
<SG>/
├── AWS-SN0_2283_usage.txt
├── <SG>-replay.json
├── <SG>.cnv.excluded_intervals.bed.gz
├── <SG>.cnv.gff3
├── <SG>.cnv.igv_session.xml
├── <SG>.cnv.vcf.gz(.md5sum|.tbi)
├── <SG>.cnv_metrics.csv
├── <SG>.cyp2d6.tsv
├── <SG>.fastqc_metrics.csv
├── <SG>.fragment_length_hist.csv
├── <SG>.hard-filtered.baf.bw
├── <SG>.improper.pairs.bw
├── <SG>.insert-stats.tab
├── <SG>.mapping_metrics.csv                      # "primary" — real values throughout
├── <SG>.pcr-model(-0).log
├── <SG>.pileup.txt
├── <SG>.ploidy.vcf.gz(.md5sum|.tbi)
├── <SG>.ploidy_estimation_metrics.csv
├── <SG>.qc-coverage-region-{1,2}_{contig_mean_cov,coverage_metrics,fine_hist,hist,overall_mean_cov}.csv
│                                                   # NOTE: no *_cov_report.bed here (2026 has it)
├── <SG>.repeats.bam
├── <SG>.repeats.vcf.gz(.tbi)
├── <SG>.roh.bed
├── <SG>.roh_metrics.csv
├── <SG>.seg(.bw|.called|.called.merged)
├── <SG>.sv.vcf.gz(.tbi)
├── <SG>.sv_metrics.csv
├── <SG>.target.counts.{bw,diploid.bw,gc-corrected.gz,gz}
├── <SG>.time_metrics.csv
├── <SG>.tn.{bw,tsv.gz}
├── <SG>.trimmer_metrics.csv
├── <SG>.vc_metrics.csv
├── <SG>.wgs_{contig_mean_cov,coverage_metrics,fine_hist,hist,overall_mean_cov}.csv
├── dragen_run_<epoch>_<pid>.log
│
├── logs/                                          # Filipino pilot ONLY
│   └── dragen_run_<epoch>_<pid>.log                #   a 2nd dragen_run log
│
├── supplemental/                                  # Filipino pilot ONLY
│   ├── <SG>-replay.json                            #   2nd replay.json, different run
│   ├── <SG>.client_profile.csv
│   ├── <SG>.dragenvc-{genprofile,graphprofile,rlprofile,roiprofile,stats}.log
│   ├── <SG>.hard-filtered.gvcf.gz.md5sum
│   ├── <SG>.mapping_metrics.csv                    #   collides w/ top-level in MultiQC staging
│   │                                                #   → wins the race; NA for contamination/
│   │                                                #     insert-length/coverage/mapping-rate
│   ├── <SG>.time_metrics.csv                        #   also collides (same basename as top-level)
│   ├── <SG>.vc_hethom_ratio_metrics.csv
│   ├── <SG>.vc_metrics.csv                          #   also collides (same basename as top-level)
│   ├── dragen.time_log.txt
│   ├── dragen_run_<epoch>_<pid>.log                 #   a 3rd dragen_run log
│   ├── streaming_log_none(1000).csv                 #   collides cohort-wide (generic name, no <SG> prefix)
│   └── sort_spill/
│       └── partitions.txt
│
└── sv/                                             # Filipino pilot ONLY
    ├── results/
    │   ├── stats/
    │   │   ├── alignmentStatsSummary.txt
    │   │   ├── candidate_metrics.csv                #   collides cohort-wide (generic name)
    │   │   ├── diploidSV.sv_metrics.csv              #   collides cohort-wide (generic name)
    │   │   ├── graph_metrics.csv                     #   collides cohort-wide (generic name)
    │   │   ├── svCandidateGenerationStats.{tsv,xml}
    │   │   └── svLocusGraphStats.tsv
    │   └── variants/
    │       ├── candidateSV.vcf.gz(.tbi)
    │       └── diploidSV.vcf.gz(.tbi)
    └── workspace/
        ├── alignmentStats.xml
        ├── chromDepth.txt
        ├── edgeRuntimeLog.txt
        ├── genomeSegmentScanDebugInfo.txt
        ├── svLocusGraph.bin
        ├── genomeDepth/
        │   ├── genome_depth.bam_0.bin
        │   └── genome_depth.bam_0.idx
        └── logs/
            └── config_log.txt
```

## Subsequent layout

```
<SG>/                                               # FLAT — no logs/, supplemental/, or sv/ at all
├── AWS-SN0_2283_usage.txt
├── AWS-SN0_2417_usage.txt                          # TWO usage files (Filipino pilot has one)
├── <SG>-replay.json
├── <SG>.cnv.excluded_intervals.bed.gz
├── <SG>.cnv.gff3
├── <SG>.cnv.igv_session.xml
├── <SG>.cnv.vcf.gz(.md5sum|.tbi)
├── <SG>.cnv_metrics.csv
├── <SG>.cram.md5sum                                # Subsequent only
├── <SG>.cyp2d6.tsv
├── <SG>.dragen.time_metrics.csv                     # Subsequent only (extra, alongside .time_metrics.csv)
├── <SG>.fastqc_metrics.csv
├── <SG>.fragment_length_hist.csv
├── <SG>.hard-filtered.baf.bw
├── <SG>.hard-filtered.gvcf.gz.md5sum                # Subsequent: top-level; Filipino pilot: only under supplemental/
├── <SG>.hard-filtered.recal.gvcf.gz.md5             # Subsequent only
├── <SG>.hard-filtered.recal.gvcf.gz.tbi.md5         # Subsequent only
├── <SG>.hard-filtered.recal.gvcfqc-shard-metrics.csv(.md5)  # Subsequent only
├── <SG>.improper.pairs.bw
├── <SG>.insert-stats.tab
├── <SG>.mapping_metrics.csv                         # ONLY copy — no collision risk
├── <SG>.pcr-model(-0).log
├── <SG>.pileup.txt
├── <SG>.ploidy.vcf.gz(.md5sum|.tbi)
├── <SG>.ploidy_estimation_metrics.csv
├── <SG>.qc-coverage-region-{1,2}_{contig_mean_cov,cov_report.bed,coverage_metrics,fine_hist,hist,overall_mean_cov}.csv
│                                                     # NOTE: includes *_cov_report.bed (Filipino pilot doesn't)
├── <SG>.repeats.bam
├── <SG>.repeats.vcf.gz(.tbi)
├── <SG>.roh.bed
├── <SG>.roh_metrics.csv
├── <SG>.seg(.bw|.called|.called.merged)
├── <SG>.sv.vcf.gz(.tbi)
├── <SG>.sv_metrics.csv                              # top-level only — no nested sv/ tree at all
├── <SG>.target.counts.{bw,diploid.bw,gc-corrected.gz,gz}
├── <SG>.time_metrics.csv                            # ONLY copy — no collision risk
├── <SG>.tn.{bw,tsv.gz}
├── <SG>.trimmer_metrics.csv
├── <SG>.vc_metrics.csv                              # ONLY copy — no collision risk
├── <SG>.wgs_{contig_mean_cov,coverage_metrics,fine_hist,hist,overall_mean_cov}.csv
└── dragen_run_<epoch>_<pid>.log                     # only ONE (Filipino pilot has three: top-level + logs/ + supplemental/)
```

59 file(s)/pattern(s) (after generalizing the `<SG>` prefix) are structurally
identical between the two vintages; everything annotated above is a verified
difference, confirmed via `gcloud storage ls -r` on one sequencing group per
vintage and a set-diff of the resulting relative paths.

## Collision impact

`run_multiqc.py`'s staging step (`gcloud storage cp -I`) copies by basename
only, discarding the source directory. For Filipino pilot sequencing groups this
means:

- `<SG>.mapping_metrics.csv`, `<SG>.time_metrics.csv`, and `<SG>.vc_metrics.csv`
  each exist twice (top-level + `supplemental/`) and collide at the same
  staging path. The `supplemental/` copy is written second and wins, but has
  `NA` values for several `MAPPING/ALIGNING SUMMARY` fields
  (`Estimated sample contamination`, `Insert length: mean/median/standard
  deviation`, `DRAGEN mapping rate [mil. reads/second]`) that the discarded
  top-level copy has real values for.
- `candidate_metrics.csv`, `diploidSV.sv_metrics.csv`, `graph_metrics.csv`, and
  `streaming_log_none(1000).csv` are generic Dragen filenames (no `<SG>`
  prefix at all) nested under `sv/results/stats/` or `supplemental/`, so every
  Filipino pilot sequencing group in a cohort collides on the exact same
  destination path — only the last one processed survives.
