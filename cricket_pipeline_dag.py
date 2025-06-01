from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add the scripts directory to Python path
sys.path.append('/home/lohit/airflow/scripts')

default_args = {
    'owner': 'lohit',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_step1():
    """Unzip data files"""
    import step1_unzipping
    # IMPORTANT: Actually call the main function!
    result = step1_unzipping.main()
    return f"Step 1 completed: {result}"

def run_step2():
    """Quality assessment pre-wrangling"""
    import step2_quality_assessment_pre
    # IMPORTANT: Actually call the main function!
    result = step2_quality_assessment_pre.main()
    return f"Step 2 completed: {result}"

def run_step3():
    """Data unnesting and processing"""
    import step3_unnesting
    # IMPORTANT: Actually call the main function!
    result = step3_unnesting.main()
    return f"Step 3 completed: {result}"

def run_step4():
    """Quality assessment post-wrangling"""
    import step4_quality_assessment_post
    # IMPORTANT: Actually call the main function!
    result = step4_quality_assessment_post.main()
    return f"Step 4 completed: {result}"

def run_step5():
    """Add features to database"""
    import step5_added_features
    # IMPORTANT: Actually call the main function!
    result = step5_added_features.main()
    return f"Step 5 completed: {result}"

with DAG(
    'cricket_data_pipeline',
    default_args=default_args,
    description='Cricket data processing pipeline',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['cricket', 'data-engineering']
) as dag:

    step1_task = PythonOperator(
        task_id='step1_unzip_data',
        python_callable=run_step1,
        doc_md="""
        ## Step 1: Unzip Data
        Extracts JSON files from the zip archive
        """
    )

    step2_task = PythonOperator(
        task_id='step2_quality_check_pre',
        python_callable=run_step2,
        doc_md="""
        ## Step 2: Pre-wrangling Quality Assessment
        Analyzes data quality before processing
        """
    )

    step3_task = PythonOperator(
        task_id='step3_unnesting_processing',
        python_callable=run_step3,
        doc_md="""
        ## Step 3: Data Unnesting
        Processes JSON data and creates normalized tables
        """
    )

    step4_task = PythonOperator(
        task_id='step4_quality_check_post',
        python_callable=run_step4,
        doc_md="""
        ## Step 4: Post-wrangling Quality Assessment
        Validates processed data quality
        """
    )

    step5_task = PythonOperator(
        task_id='step5_feature_engineering',
        python_callable=run_step5,
        doc_md="""
        ## Step 5: Feature Engineering
        Adds calculated features to the database
        """
    )

    # Define task dependencies
    step1_task >> step2_task >> step3_task >> step4_task >> step5_task