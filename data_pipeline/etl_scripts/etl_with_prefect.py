from prefect import flow, task
import pandas as pd
import sqlalchemy
import smtplib
from email.message import EmailMessage
import time

@task(retries=3, retry_delay_seconds=10)
def extract_data():
    print("Extracting data...")
    data = pd.DataFrame({'column1': [1, 2, 3], 'column2': ['A', 'B', 'C']})
    return data

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
    # Replace with your email details
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

@flow(name="ETL Flow with Notifications")
def etl_flow():
    try:
        data = extract_data()
        transformed_data = transform_data(data)
        load_data(transformed_data)
        send_notification("completed successfully")
    except Exception as e:
        send_notification("failed")
        raise e  # Re-raise the exception to ensure the flow fails as expected

if __name__ == "__main__":
    etl_flow()

