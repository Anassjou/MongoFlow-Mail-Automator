# Departmental Data Management Workflow

This project utilizes Apache Airflow to orchestrate a workflow for extracting, transforming, and sending department-specific data reports via email. The data is sourced from a MongoDB database and this process runs on a scheduled basis every week.

## Project Overview

The workflow is designed to:
- **Extract** data from a MongoDB database containing information about different departments within a company.
- **Transform** the data to calculate the age of employees and format the data into monthly start date periods.
- **Send** an automated email with the transformed data as an attachment to the respective department manager.

The entire process is intended to streamline data flow management and enhance transparency within the organization.

## Installation

1. **Prerequisites**:
   - MongoDB database with access permissions.
   - Apache Airflow environment.

2. **Clone the repository**:
   ```bash
   git clone <repository-url>
   ```

3. **Setup MongoDB Connection**:
    - Ensure that MongoDB is accessible from your Airflow instance.
    - Update the cluster_URL in the DAG file to the URL of your MongoDB cluster.

4. **Configure Airflow**:
    - Place the DAG file in your Airflow DAGs folder.
    - Ensure the dependencies are installed:
    ```bash
    pip install pymongo pandas
    ```

## Running the Workflow
To start the workflow, ensure that Apache Airflow is running and the DAG is enabled in the Airflow UI. The DAG is scheduled to run weekly, but you can manually trigger the DAG to run at any time from the Airflow UI.

## DAG Structure
- DAG ID: project_dag
- Tasks:
    - fetch_IT_data: Extracts and transforms data for the IT department.
    - fetch_HR_data: Extracts and transforms data for the HR department.
    - send_email1: Sends an email with the IT department's data report.
    - send_email2: Sends an email with the HR department's data report.
- Dependencies:
    - fetch_IT_data >> send_email1
    - fetch_HR_data >> send_email2

## Additional Notes
  Ensure that the email addresses in the EmailOperator tasks are updated to the actual email addresses of the department managers.
  The data processing script assumes all necessary fields are present and correctly formatted in the MongoDB database.
