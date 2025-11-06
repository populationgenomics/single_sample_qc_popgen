FROM australia-southeast1-docker.pkg.dev/cpg-common/images/cpg_hail_gcloud:0.2.134.cpg2-1

# DeepVariant pa pipeline version.
ENV VERSION=0.1.4

# Add in the additional requirements that are most likely to change.
COPY LICENSE pyproject.toml README.md ./
COPY src src/
# COPY third_party third_party/
COPY gnomad_methods/gnomad gnomad

RUN pip install .[cpg]
