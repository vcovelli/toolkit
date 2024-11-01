from prefect import flow, task
import pandas as pd
import sqlalchemy
import smtplib
from email.message import EmailMessage

@task(retries=3, retry_delay_seconds=10)
def extract_data():
    print("Extracting data...")
    data = pd.DataFrame({'column1': [1, 2, 3], 'column2': ['A', 'B', 'C']})
    return data

@task
def validate_data(data):
    print("Validating data...")

    # Check for missing values
    if data.isnull().values.any():
        raise ValueError("Data contains missing values.")

    # Check data types (example: column1 should be int, column2 should be string)
    if not pd.api.types.is_integer_dtype(data['column1']):
        raise TypeError("column1 is not of type int.")
    if not pd.api.types.is_string_dtype(data['column2']):
        raise TypeError("column2 is not of type string.")
    
    # Additional validation checks can be added as needed
    print("Data validation passed.")

@task(retries=3, retry_delay_seconds=10)
def transform_data(data):
    print("Transforming data...")
    data['column3'] = data['column1'] * 2
    return data

@task(retries=3, retry_delay_seconds=10)
def load_data(data):
    print("Loading data...")
    connection_string = "postgresql://your_username:your_password@localhost:5432/your_database"
    engine = sqlalchemy.create_engine(connection_string)
    data.to_sql('example_table', con=engine, if_exists='replace', index=False)

@task
def send_notification(status):
    email_sender = 'your_email@example.com'
    email_password = 'your_email_password'
    email_receiver = 'receiver_email@example.com'

    subject = f"ETL Flow {status}"
    body = f"The ETL flow has {status}."

    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = email_sender
    msg['To'] = email_receiver

    try:
        with smtplib.SMTP_SSL('smtp.example.com', 465) as smtp:
            smtp.login(email_sender, email_password)
            smtp.send_message(msg)
        print(f"Notification sent: ETL Flow {status}")
    except Exception as e:
        print(f"Failed to send notification: {e}")

@flow(name="ETL Flow with Validation and Notifications")
def etl_flow():
    try:
        data = extract_data()
        validate_data(data)  # Run data validation after extraction
        transformed_data = transform_data(data)
        load_data(transformed_data)
        send_notification("completed successfully")
    except Exception as e:
        send_notification("failed")
        raise e  # Re-raise the exception to ensure the flow fails as expected

if __name__ == "__main__":
    etl_flow()

