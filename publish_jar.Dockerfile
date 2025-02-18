FROM houpy0829/chronon-ci:base--f87f50dc520f7a73894ae024eb78bd305d5b08e2

COPY . /workspace

WORKDIR /workspace

# Initialize conda and activate environment in the same shell
SHELL ["/bin/bash", "-c"]
ENV SBT_OPTS="-Xmx4G -Xms2G"
RUN source /opt/conda/etc/profile.d/conda.sh && \
    conda init bash && \
    conda activate chronon_py

ENTRYPOINT ["/bin/bash"]%