FROM apache/airflow:2.10.5

# Set working directory
WORKDIR /opt/airflow

# Install Poetry
RUN pip install poetry

# Copy project files
COPY pyproject.toml poetry.lock /opt/airflow/

# Install dependencies
RUN poetry config virtualenvs.create false \
    && poetry install --no-root --no-dev