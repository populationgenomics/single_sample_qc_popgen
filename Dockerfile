FROM australia-southeast1-docker.pkg.dev/cpg-common/images/cpg_hail_gcloud:0.2.134.cpg1

ENV VERSION=0.0.1

# Add in the additional requirements that are most likely to change.
COPY LICENSE pyproject.toml README.md ./
COPY src src/

RUN pip install .[cpg]
