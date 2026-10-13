FROM apache/airflow:2.10.5-python3.11
USER root
RUN sed -i 's|http://deb.debian.org|https://deb.debian.org|g' /etc/apt/sources.list.d/debian.sources \
 && apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg curl \
 && rm -rf /var/lib/apt/lists/*
USER airflow
# Lab code (deltalake, DuckDB, dbt) lives in its own venv: no clashes with Airflow's dependency pins.
COPY docker/requirements-lab.txt /tmp/requirements-lab.txt
RUN python -m venv /home/airflow/lab-venv \
 && env -u PIP_CONSTRAINT -u PIP_REQUIRE_HASHES /home/airflow/lab-venv/bin/pip install --no-cache-dir --default-timeout=300 --retries=10 -r /tmp/requirements-lab.txt \
 && /home/airflow/lab-venv/bin/python -c "import duckdb; c = duckdb.connect(); [c.execute(f'INSTALL {e}') for e in ('httpfs','delta','postgres')]"
COPY --chmod=755 docker/airflow-entrypoint.sh /airflow-entrypoint.sh
ENTRYPOINT ["/airflow-entrypoint.sh"]
