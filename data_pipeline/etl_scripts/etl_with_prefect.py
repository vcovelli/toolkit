from prefect import flow, task
import pandas as pd
import sqlalchemy

@task
def extract_data():
    print("Extracting data...")
    # Placeholder for data extraction logic
    data = pd.DataFrame({'column1': [1, 2, 3], 'column2': ['A', 'B', 'C']})
    return data

@task
def transform_data(data):
    print("Transforming data...")
    # Placeholder for data transformation logic
    data['column3'] = data['column1'] * 2
    return data

@task
def load_data(data):
    print("Loading data...")
    # Placeholder for data loading logic
    # Example connection string for PostgreSQL
    connection_string = "postgresql://user:password@localhost:5432/database"
    engine = sqlalchemy.create_engine(connection_string)
    data.to_sql('example_table', con=engine, if_exists='replace', index=False)

@flow(name="ETL Flow")
def etl_flow():
    data = extract_data()
    transformed_data = transform_data(data)
    load_data(transformed_data)

if __name__ == "__main__":
    # Run the flow
    etl_flow()

