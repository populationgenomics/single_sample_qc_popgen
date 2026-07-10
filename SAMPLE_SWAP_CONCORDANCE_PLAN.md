# Plan: Fix pgen resolution for WGS↔array sample-swap concordance

> **Status (2026-07-10): proposal for team review.** Captures the diagnosis of
> why `single_sample_qc_popgen` runs fail after image tag `0.1.5-8`, the
> evidence gathered from Metamist and Hail Batch, and a recommended fix. The
> selection logic and the upstream Metamist question both need sign-off before
> implementation.

## TL;DR

There are two separate problems, one already fixed:

1. **Fixed — Metamist query crash.** `QUERY_ARRAY_PGEN` selected `outputs { path }`,
   but `analysis.outputs` is a `JSON` scalar with no subfields, so the query is
   rejected at validation time and the driver dies during workflow planning.
   The fix (select `outputs`, read `path` from the JSON in Python) is on branch
   `fix-array-pgen-outputs-query`.

2. **Open — pgen file resolution is scoped to the wrong cohort.** `query_array_pgen_path`
   looks for the `array_aggregate_pgen` analysis registered against the **WGS
   cohort** passed in config. It is never registered there. It is registered
   against **genotyping-array cohorts**, which are a structurally different set.
   Even with problem 1 fixed, planning still fails with
   `No array_aggregate_pgen analysis registered in metamist for cohort <WGS cohort>`.

Both were introduced together in commit `00c9ff1`, which replaced a
config-supplied pgen path with a Metamist lookup.

## Context

The swap-check compares WGS genotypes against array genotypes for the same
biological sample, to catch sample swaps. It needs the rolling
`array_aggregate_pgen` PLINK2 fileset produced by the popgen-genotyping
pipeline (`.pgen`/`.pvar`/`.psam`); the `.psam` lists the array sequencing
groups present in that export.

Two Metamist links are involved, and only one is broken:

- **WGS SG → array SG, via shared sample.** `QUERY_WGS_TO_ARRAY_MAPPING` walks
  `sequencingGroup → sample → array sequencingGroups`. This is correct and
  works on the WGS cohort today. It does **not** assume the cohort contains
  array SGs.
- **Cohort → `array_aggregate_pgen` analysis.** `query_array_pgen_path` assumes
  the pgen analysis is registered against the cohort under QC. This is the
  broken assumption. The concordance logic itself is sound; only the file
  resolution is wrong.

## Why the cohort-scoped lookup cannot work

Arrays are genotyped well before WGS. Array cohorts are batched by plate; WGS
cohorts by sequencing submission. The two partitions cut the sample space
differently and never line up, so there is no WGS cohort (nor a paired
WGS+array cohort you might construct) that has an `array_aggregate_pgen`
registered against it. The producer cohort and the consumer cohort are
different by construction.

## Evidence gathered

**The swap-check works end-to-end in test, on a fixture.** On 2026-06-19,
against ourdna-test cohort COH13529 ("Sample Swap Testing Cohort. WGS SGs only",
10 WGS SGs, **no registered analyses**), workflow batches 1133155 and 1133161
ran all five stages green: `PrepareSampleSwap → SwapCheckExportVcf →
SwapCheckSomalierExtract → SwapCheckSomalierRelate → SwapCheckClassify`. They
succeeded because the pgen was supplied via the test-only `dev_override` config,
pointing at a hand-made psam
(`gs://cpg-ourdna-test/COH13537_swample_swap_test_plink/test_subset_swapped.psam`),
not a rolling aggregate. So the somalier/classify machinery is proven; the one
thing never exercised is **production pgen resolution** — the Metamist lookup
against a real `array_aggregate_pgen`.

**It has never run in production.** Swap-check landed after `0.1.5-8`. Every
green production run — batches 1133390, 1133451, 1133453, 1133454, 1133510,
1133511 (alex.stuckey, image `0.1.5-7`), 1128782 (michael.harper, repo commit
`87cf142`), and 1133901 (image `0.1.5-8`) — is **MultiQC-based QC only**:
`RunMultiQc → CheckMultiQc → RegisterQcMetrics`. None contain
`PrepareSampleSwap`, somalier, or pgen. Production concordance has produced zero
results.

**The `array_aggregate_pgen` exports are cumulative nested supersets.** For the
ourdna project, sample counts in each export's `.psam`:

| analysis | completed | run dir | .psam samples | cohorts registered |
|---|---|---|---|---|
| 344641 | 2026-03-30 | `/1/` | 190 | 2 |
| 344844 | 2026-03-31 | `/1/` | 190 | 2 |
| 344852 | 2026-04-01 | `/2/` | 285 | 1 (COH10269) |
| 345064 | 2026-04-02 | `/3/` | 2470 | 26 |

Verified containment: `190 ⊆ 285 ⊆ 2470`, with zero samples from earlier
exports missing from the latest. So the newest export already contains every
array sample any earlier export had.

**Registration edges do not track content.** Run 2 (285 samples, spanning
COH10152/COH10806's samples) is registered against **COH10269 only**.
Registration follows "which cohort the export run was invoked for," not "every
cohort whose samples are inside." This is why traversing WGS SG → array SG →
array cohort → registered pgen is unreliable: an array SG can sit inside a pgen
whose analysis is not registered against that SG's cohort.

**Superseded exports are left active.** All four analyses are `active=True`,
including the 190- and 285-sample intermediates. Nothing deactivates an
aggregate when a newer one supersedes it.

## Recommended fix

### Consumer side (this repo): resolve the latest active aggregate, project-scoped

Replace the cohort-scoped lookup with a project-scoped one that is forward
compatible with the upstream fix below:

```
query the ourdna project for array_aggregate_pgen where active = True
  0 results  -> raise (no aggregate registered yet)
  1 result   -> use it                          # post-upstream-fix world
  >1 results -> take max(timestampCompleted); log a warning listing the
                ids/timestamps chosen and skipped
derive .pvar/.psam from the .pgen (existing suffix swap)
```

Then the existing per-SG check runs unchanged: for each WGS SG, is its paired
array SG present in the `.psam`? This yields `ready` /
`array_pending_export` / `multiple_array_sgs`, and genotype concordance runs
only on the `ready` samples.

Properties:

- **Works today.** Multiple active aggregates → newest, which is the verified
  superset.
- **Self-heals on the upstream fix.** Once superseded exports are deactivated,
  the `>1` branch stops being taken; the `==1` branch handles it with no code
  change. Filtering `active=True` in the query is what makes that automatic.
- **Coverage is graceful.** A sample whose plate has not yet been folded into
  the latest export lands as `array_pending_export`, not a wrong concordance
  call. A re-run after the next export picks it up.

Residual assumption to document in the code: the `>1` branch is correct only
while multiple-active exports are cumulative supersets. True today; irrelevant
once single-active is enforced. The per-SG psam check softens even a violation
(uncovered samples deferred, never silently mismatched).

### Upstream question for the popgen-genotyping team

Why are older `array_aggregate_pgen` analyses left active when a newer rolling
export supersedes them? Two ways to make "the current aggregate" unambiguous:

- **Exactly one active aggregate per project** — deactivate superseded exports
  when a new one is registered, or
- **Exactly one active aggregate per genotyping cohort.**

Setting an analysis inactive is supported today: `PATCH updateAnalysis` with
`{"active": false}` (requires full write access). So this is a policy gap, not
a technical blocker. If either is adopted, the consumer's `==1` branch becomes
the normal path and timestamp reasoning disappears.

Decide: was leaving them active an oversight, or is there a reason downstream
consumers depend on old aggregates remaining active?

### Config override: exists, but test-only by design

`resolve_array_pgen_paths` (`stages.py`) already has a config path —
`[workflow.swap_check].dev_override` with `pgen_path`/`pvar_path`/`psam_path`
(added in `d7cd709`). But it **raises unless access level is `test`**;
production always falls through to the Metamist lookup. So there is, by design,
no config escape in production, and it cannot unblock a full-access cohort like
COH14170.

The team should decide whether production should be allowed a config path at
all (relax the access gate to a documented emergency override), or rely solely
on the Metamist lookup once the project-scoped fix lands. Leave the test-only
`dev_override` as-is regardless — it is what the June-19 test runs used.

## Files to modify (consumer fix)

| File | Change |
|------|--------|
| `src/single_sample_qc_popgen/metamist_utils.py` | Replace `query_array_pgen_path(cohort_id)` with a project-scoped `query_latest_array_pgen_path(project)`: query `array_aggregate_pgen` filtered `active=True`, apply the 0/1/>1 selection, log on `>1`. Update the module docstring (it currently states the aggregate is registered against the cohort — the opposite of reality). |
| `src/single_sample_qc_popgen/stages.py` | In `resolve_array_pgen_paths`, pass the dataset/project name (e.g. from the cohort's dataset) to the project-scoped query instead of `cohort.id`. Leave the test-only `dev_override` branch unchanged. |
| `test/` | Cover 0 / 1 / >1-active selection and the psam-membership classification against a fixture psam. |

## Immediate options for COH14170

There is no clean no-code production unblock: `dev_override` is test-only, and
`0.1.5-7`/`0.1.5-8` have no swap-check stage at all. Choices:

- **QC only now, swap-check later** — run `0.1.5-8` (what production already
  does) to get MultiQC QC on COH14170; defer concordance until the fix lands.
- **Land the consumer fix** — the project-scoped latest-active change is small
  and unblocks concordance for real cohorts without touching the test path.
- **Reproduce in test first** — run the full chain under ourdna-test with
  `dev_override` pointed at the latest real export
  (`.../ExportCohortDatasets/3/20260402_cohort.{pgen,pvar,psam}`) to validate
  the somalier/classify stages against real aggregate data before the prod fix.

## Open questions for Monday

1. Upstream: single-active-aggregate policy — adopt, and per-project or
   per-cohort? Oversight or intentional?
2. Do we want the Metamist lookup at all, or keep config-supplied paths given
   production has only ever run without the lookup?
3. Project scope: is "latest active `array_aggregate_pgen` in the ourdna
   project" the right universe, or should it be constrained further?
