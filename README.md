# Euroleague Fantasy Data Pipeline

The Euroleague Fantasy Data Pipeline is an end-to-end solution designed to prepare data for training a recommendation model tailored to Euroleague Fantasy. This pipeline fetches data from the [Euroleague API](https://live.euroleague.net/api), stores the raw data in an AWS S3 bucket, transforms it, and loads it into a PostgreSQL database hosted on AWS RDS. Database migrations are handled using Alembic, and the entire workflow is orchestrated with Apache Airflow running in Docker. Dependencies are managed with Poetry.

---

## Pipeline Overview

- **Data Ingestion:**  
  Data is fetched from the Euroleague API.

- **Data Storage:**  
  Raw data is stored in an AWS S3 bucket for durability and further processing.

- **Data Transformation & Loading:**  
  The data is transformed and loaded into a PostgreSQL database hosted on AWS RDS.

- **Database Migrations:**  
  Alembic is used to handle schema migrations, ensuring consistency and version control.

- **Workflow Automation:**  
  Apache Airflow orchestrates and schedules the pipeline tasks via Docker Compose.

- **Next Steps:**  
  The transformed data in PostgreSQL serves as the foundation for training a recommendation model for Euroleague Fantasy.

---

## Prerequisites

- **Python:** Version 3.8 or higher.
- **AWS Account:** Access to S3 and RDS services.
- **PostgreSQL:** For local testing or direct usage.
- **Docker & Docker Compose:** To run Apache Airflow and related services.
- **Poetry:** For dependency management and project setup.
- **Alembic:** For database migrations.
- **Other Dependencies:** Managed via Poetry.

---

## Setup Instructions

### 1. Clone the Repository

Clone the repository to your local machine:

```bash
git clone https://github.com/yourusername/euroleague-fantasy-data-pipeline.git
cd euroleague-fantasy-data-pipeline
```

### 2. Configure Environment Variables

- **Environment Configuration:**  
  Copy the example environment file and update the placeholder values with your AWS credentials, S3 bucket name, PostgreSQL connection details, and any other necessary configurations.

  ```bash
  cp .env.example .env
  ```

- **Alembic Configuration:**  
  Similarly, copy the Alembic example configuration file and modify it according to your PostgreSQL settings.

  ```bash
  cp alembic.ini.example alembic.ini
  ```

### 3. Install Dependencies with Poetry

Make sure you have [Poetry](https://python-poetry.org/) installed. Then, install the project dependencies:

```bash
poetry install
```

To activate the virtual environment managed by Poetry, run:

```bash
poetry shell
```

### 4. Configure and Run Apache Airflow with Docker Compose

A Docker Compose file is provided to run Apache Airflow. Initialize Airflow and start the services by running:

```bash
docker compose run airflow-init
docker compose up -d
```

These commands will initialize the Airflow environment and start the Airflow scheduler, webserver, and other necessary components in Docker containers. You can access the Airflow UI at `http://localhost:8080`.

---

## Running the Pipeline

### Data Ingestion & Transformation

- **Airflow DAGs:**  
  The pipeline’s tasks are defined as DAGs in the Airflow configuration. Once Airflow is running via Docker Compose, use the Airflow UI to trigger the pipeline. The process includes:
  - Fetching data from the Euroleague API.
  - Storing the raw data in your specified S3 bucket.
  - Transforming and loading the data into the PostgreSQL database on AWS RDS.

### Database Migrations

- **Running Migrations:**  
  Use Alembic to apply database migrations and ensure your PostgreSQL schema is up-to-date:

  ```bash
  alembic upgrade head
  ```

---

## Future Development: Model Training

This pipeline lays the groundwork for your recommendation system. Once the data is available in PostgreSQL, you can build and train your recommendation model using your preferred machine learning framework. Future updates to the repository will include detailed scripts and instructions for model training.


---

## Contributing

Contributions are welcome! If you wish to improve the project, please:
- Fork the repository.
- Create a feature branch.
- Submit a pull request with your changes.

Ensure you follow the code style guidelines and add tests where applicable.

---

## License

This project is licensed under the [Apache License 2.0](LICENSE).

---

## Contact

For questions, issues, or suggestions, please open an issue in the GitHub repository or contact the maintainers directly.

Happy Coding!