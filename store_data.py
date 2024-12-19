import psycopg2
import pandas as pd

# Load CSV into DataFrame
csv_file = 'data/doctor_dataset.csv'
df = pd.read_csv(csv_file)

# Preprocess the column names: strip spaces, remove special characters
df.columns = df.columns.str.strip()  # Remove leading/trailing spaces
df.columns = df.columns.str.replace(r"[^\w\s]", "", regex=True)  # Remove special characters
df.columns = df.columns.str.replace(r"\s+", "_", regex=True)  # Replace spaces with underscores

# Fill missing values with None for numeric columns and empty string for text columns
for column in df.columns:
    if df[column].dtype in ['int64', 'float64']:
        df[column] = df[column].fillna(0)
    else:
        df[column] = df[column].fillna('')

# Print processed column names for debugging
print("Processed column names:")
print(df.columns)

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="doctor_database", user="postgres", host="localhost", port="5432"
)

cursor = conn.cursor()

# Specify the table name and columns manually (since you know them)
table_name = "doctors"
columns = df.columns

# Helper function to determine column type
def infer_column_type(value):
    if isinstance(value, str):
        return "TEXT"
    elif isinstance(value, (int, float)):
        return "INTEGER" if isinstance(value, int) else "FLOAT"
    else:
        return "TEXT"

# Create table query using fixed columns
create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
for column in columns:
    column_type = infer_column_type(df[column].iloc[0])
    print(f"Column '{column}' type: {column_type}")
    # Quote column names to avoid errors with special characters
    create_table_query += f"\"{column}\" {column_type}, "

create_table_query = create_table_query.rstrip(', ') + ");"  # Remove trailing comma and add closing parenthesis

# Execute the table creation query
cursor.execute(create_table_query)

# Insert CSV data into PostgreSQL
insert_query = "INSERT INTO {} ({}) VALUES ({})".format(
    table_name,
    ', '.join([f'\"{col}\"' for col in columns]),
    ', '.join(['%s'] * len(columns))
)

for index, row in df.iterrows():
    # Extract row values using column names
    row_values = tuple(row[col] if pd.notna(row[col]) else None for col in columns)

    # Execute insert query
    cursor.execute(insert_query, row_values)

# Commit and close connection
conn.commit()
cursor.close()
conn.close()

print("CSV data inserted into PostgreSQL.")
