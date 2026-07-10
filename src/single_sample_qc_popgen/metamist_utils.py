"""
Metamist GraphQL query helpers for SwapCheck.

The WGS-to-array sequencing-group mapping for a cohort is fetched via
``query_wgs_to_array_mapping`` (each WGS SG has zero or more associated
active array SGs via shared sample).

The rolling popgen-genotyping pgen path is looked up via
``query_array_pgen_path`` -- the ``array_aggregate_pgen`` analysis is
registered against the cohort in metamist and its ``outputs.path`` points
at the ``.pgen``. The sibling ``.pvar``/``.psam`` live alongside it and are
derived by ``derive_pgen_sibling_paths`` (a plain suffix swap).

Classification (``classify_wgs_to_array_mapping``) is pure logic so it can
be unit-tested without a metamist round-trip.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cpg_utils import to_path
from metamist.graphql import gql, query

if TYPE_CHECKING:
    from cpg_utils import Path


# WGS -> array mapping, scoped to a single cohort.
QUERY_WGS_TO_ARRAY_MAPPING = gql(
    """
    query SwapCheckMapping($cohortId: String!) {
      cohorts(id: {eq: $cohortId}) {
        sequencingGroups {
          id
          sample {
            id
            externalId
            sequencingGroups(
              type: {eq: "genotypingarray"}
              activeOnly: {eq: true}
            ) {
              id
            }
          }
        }
      }
    }
    """
)


def query_wgs_to_array_mapping(cohort_id: str) -> list[dict[str, Any]]:
    """Run the metamist query and flatten to one record per WGS SG.

    Each record carries the WGS SG ID, sample external ID, sample-present
    flag (defensive -- should always be true), and the list of candidate
    array SG IDs for the same biological sample. Classification against the
    pgen psam happens in ``classify_wgs_to_array_mapping``.
    """
    response = query(QUERY_WGS_TO_ARRAY_MAPPING, variables={'cohortId': cohort_id})
    cohorts = response.get('cohorts') or []
    if not cohorts:
        raise RuntimeError(f'No cohort returned from metamist for id {cohort_id}')
    cohort_sgs = cohorts[0]['sequencingGroups']

    records: list[dict[str, Any]] = []
    for sg in cohort_sgs:
        sample = sg.get('sample') or {}
        array_candidates = [s['id'] for s in (sample.get('sequencingGroups') or [])]
        records.append(
            {
                'wgs_sg': sg['id'],
                'sample_external_id': sample.get('externalId'),
                'sample_present': bool(sample),
                'array_candidates': array_candidates,
            }
        )
    return records


# Rolling array_aggregate_pgen analysis registered against the cohort.
QUERY_ARRAY_PGEN = gql(
    """
    query ArrayPgenPath($cohortId: String!) {
      cohorts(id: {eq: $cohortId}) {
        analyses {
          id
          type
          timestampCompleted
          outputs
        }
      }
    }
    """
)


def query_array_pgen_path(cohort_id: str) -> str:
    """Return the GCS path to the cohort's latest ``array_aggregate_pgen`` pgen.

    The popgen-genotyping export is registered against the cohort as an
    ``array_aggregate_pgen`` analysis whose ``outputs.path`` is the ``.pgen``.
    If several are registered (re-exports), the most recently completed wins.
    The sibling ``.pvar``/``.psam`` are derived via ``derive_pgen_sibling_paths``.
    """
    response = query(QUERY_ARRAY_PGEN, variables={'cohortId': cohort_id})
    cohorts = response.get('cohorts') or []
    if not cohorts:
        raise RuntimeError(f'No cohort returned from metamist for id {cohort_id}')

    pgen_analyses = [a for a in (cohorts[0].get('analyses') or []) if a.get('type') == 'array_aggregate_pgen']
    if not pgen_analyses:
        raise RuntimeError(
            f'No array_aggregate_pgen analysis registered in metamist for cohort {cohort_id}'
        )

    # Most recently completed wins; fall back to analysis id when timestamps tie.
    latest = max(pgen_analyses, key=lambda a: (a.get('timestampCompleted') or '', a.get('id') or 0))
    outputs = latest.get('outputs') or {}
    path = outputs.get('path')
    if not path:
        raise RuntimeError(
            f'array_aggregate_pgen analysis {latest.get("id")} for cohort {cohort_id} has no output path'
        )
    return path


def derive_pgen_sibling_paths(pgen_path: str) -> tuple[str, str, str]:
    """Given the ``.pgen`` path, return ``(pgen, pvar, psam)``.

    plink2 writes the trio side by side sharing a stem, so the ``.pvar`` and
    ``.psam`` are a plain suffix swap off the ``.pgen``.
    """
    if not pgen_path.endswith('.pgen'):
        raise ValueError(f'Expected a .pgen path, got: {pgen_path}')
    stem = pgen_path.removesuffix('.pgen')
    return pgen_path, f'{stem}.pvar', f'{stem}.psam'


def read_psam_array_sgs(psam_path: str | Path) -> set[str]:
    """Read a plink2 psam and return the set of IIDs.

    Handles both local and cloud (gs://) paths via cpg_utils.to_path.
    The header line begins with '#' and lists column names; the IID column
    is required.
    """
    path = to_path(psam_path)
    with path.open() as f:
        lines = f.read().splitlines()
    if not lines:
        raise RuntimeError(f'psam is empty: {psam_path}')

    header_cols = lines[0].lstrip('#').split()
    if 'IID' not in header_cols:
        raise RuntimeError(f'No IID column in psam header: {header_cols}')
    iid_idx = header_cols.index('IID')

    sg_ids: set[str] = set()
    for line in lines[1:]:
        if not line.strip() or line.startswith('#'):
            continue
        cols = line.split()
        sg_ids.add(cols[iid_idx])
    return sg_ids


def classify_wgs_to_array_mapping(
    records: list[dict[str, Any]],
    pgen_psam_sgs: set[str],
) -> list[dict[str, Any]]:
    """Apply readiness classification to each WGS SG.

    Statuses:
        ready                  -- single active array SG and it's in the pgen
        ready (disambiguated)  -- multiple active array SGs, exactly one in pgen
        array_pending_export   -- single active array SG, not yet in the pgen
        multiple_array_sgs     -- multiple active array SGs, none in pgen
        no_array_sg            -- sample has no active array SG
        missing_sample         -- WGS SG has no linked sample (defensive)
    """
    out: list[dict[str, Any]] = []
    for r in records:
        cands: list[str] = r['array_candidates']
        in_pgen = [s for s in cands if s in pgen_psam_sgs]
        array_sg: str | None = None
        candidates: list[str] = []
        notes = ''

        if not r['sample_present']:
            status = 'missing_sample'
        elif len(cands) == 0:
            status = 'no_array_sg'
        elif len(cands) == 1:
            array_sg = cands[0]
            status = 'ready' if array_sg in pgen_psam_sgs else 'array_pending_export'
        elif len(in_pgen) == 1:
            array_sg = in_pgen[0]
            status = 'ready'
            candidates = cands
            notes = f'disambiguated from {len(cands)} candidates by pgen membership'
        else:
            status = 'multiple_array_sgs'
            candidates = cands

        out.append(
            {
                'wgs_sg': r['wgs_sg'],
                'sample_external_id': r['sample_external_id'],
                'array_sg': array_sg,
                'status': status,
                'candidates': candidates,
                'notes': notes,
            }
        )
    return out
