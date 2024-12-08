#!/usr/bin/env python
# coding: utf-8

# # Create database

# In[90]:


import psycopg

# Database connection configuration
conn = psycopg.connect(
    host="localhost",
    port="5432",
    dbname="SQL_Project",
    user="postgres",
    password="123"
)

# Function to create tables with drop if exists
def create_tables(conn):
    commands = [
        """
        DROP TABLE IF EXISTS offices CASCADE;
        CREATE TABLE offices (
            office_id CHAR(200) PRIMARY KEY,
            office_name VARCHAR(100),
            street_address VARCHAR(200),
            city VARCHAR(100),
            state CHAR(200),
            zipcode CHAR(500)
        );
        """,
        """
        DROP TABLE IF EXISTS clients CASCADE;
        CREATE TABLE clients (
            client_id CHAR(200) PRIMARY KEY,
            client_name VARCHAR(200),
            street VARCHAR(100),
            city VARCHAR(100),
            state CHAR(200),
            zipcode CHAR(500),
            email VARCHAR(1000),
            phone_number CHAR(100),
            comments VARCHAR(2000)
        );
        """,
        """
        DROP TABLE IF EXISTS property_type CASCADE;
        CREATE TABLE property_type (
            property_type_id CHAR(200) PRIMARY KEY,
            property_type_description VARCHAR(1000)
        );
        """,
        """
        DROP TABLE IF EXISTS property_status CASCADE;
        CREATE TABLE property_status (
            property_status_id CHAR(200) PRIMARY KEY,
            property_status_description VARCHAR(100)
        );
        """,
        """
        DROP TABLE IF EXISTS neighborhoods CASCADE;
        CREATE TABLE neighborhoods (
            neighborhood_id CHAR(200) PRIMARY KEY,
            neighborhood_name VARCHAR(100)
        );
        """,
        """
        DROP TABLE IF EXISTS school_district CASCADE;
        CREATE TABLE school_district (
            school_district_id CHAR(200) PRIMARY KEY,
            school_district_name VARCHAR(100)
        );
        """,
        """
        DROP TABLE IF EXISTS agents CASCADE;
        CREATE TABLE agents (
            agent_id CHAR(200) PRIMARY KEY,
            office_id CHAR(200),
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            dob DATE,
            hire_date DATE,
            email VARCHAR(100),
            phone_number CHAR(100),
            FOREIGN KEY (office_id) REFERENCES offices(office_id)
        );
        """,
        """
        DROP TABLE IF EXISTS properties CASCADE;
        CREATE TABLE properties (
            property_id CHAR(200) PRIMARY KEY,
            property_name VARCHAR(100),
            street VARCHAR(100),
            city VARCHAR(100),
            state CHAR(20),
            zipcode CHAR(50),
            neighborhood_id CHAR(200),
            property_type_id CHAR(200),
            property_status_id CHAR(200),
            school_district_id CHAR(200),
            days_on_market SMALLINT,
            listing_price NUMERIC(300,2),
            size SMALLINT,
            comments VARCHAR(2000),
            FOREIGN KEY (neighborhood_id) REFERENCES neighborhoods(neighborhood_id),
            FOREIGN KEY (property_type_id) REFERENCES property_type(property_type_id),
            FOREIGN KEY (property_status_id) REFERENCES property_status(property_status_id),
            FOREIGN KEY (school_district_id) REFERENCES school_district(school_district_id)
        );
        """,
        """
        DROP TABLE IF EXISTS appointments CASCADE;
        CREATE TABLE appointments (
            agent_id CHAR(200),
            client_id CHAR(200),
            property_id CHAR(200),
            appointment_time TIMESTAMP,
            comments VARCHAR(2000),
            PRIMARY KEY (agent_id, client_id, property_id, appointment_time),
            FOREIGN KEY (agent_id) REFERENCES agents(agent_id),
            FOREIGN KEY (client_id) REFERENCES clients(client_id),
            FOREIGN KEY (property_id) REFERENCES properties(property_id)
        );
        """,
        """
        DROP TABLE IF EXISTS transactions CASCADE;
        CREATE TABLE transactions (
            client_id CHAR(200),
            property_id CHAR(200),
            transaction_date TIMESTAMP,
            agent_id CHAR(200),
            transaction_amount NUMERIC(200,2),
            PRIMARY KEY (client_id, property_id, transaction_date),
            FOREIGN KEY (client_id) REFERENCES clients(client_id),
            FOREIGN KEY (property_id) REFERENCES properties(property_id),
            FOREIGN KEY (agent_id) REFERENCES agents(agent_id)
        );
        """,
        """
        DROP TABLE IF EXISTS expenses CASCADE;
        CREATE TABLE expenses (
            expense_id CHAR(200) PRIMARY KEY,
            office_id CHAR(200),
            agent_id CHAR(200),
            expense_item SMALLINT,
            quantity NUMERIC(300,2),
            unit_price NUMERIC(300,2),
            total_expensed_amount NUMERIC(300,2),
            FOREIGN KEY (office_id) REFERENCES offices(office_id),
            FOREIGN KEY (agent_id) REFERENCES agents(agent_id)
        );
        """,
        """
        DROP TABLE IF EXISTS amenities CASCADE;
        CREATE TABLE amenities (
            amenity_id CHAR(200) PRIMARY KEY,
            amenity_name VARCHAR(100)
        );
        """,
        """
        DROP TABLE IF EXISTS property_amenity CASCADE;
        CREATE TABLE property_amenity (
            amenity_id CHAR(200),
            property_id CHAR(200),
            PRIMARY KEY (amenity_id, property_id),
            FOREIGN KEY (amenity_id) REFERENCES amenities(amenity_id),
            FOREIGN KEY (property_id) REFERENCES properties(property_id)
        );
        """,
        """
        DROP TABLE IF EXISTS schools CASCADE;
        CREATE TABLE schools (
            school_id CHAR(200) PRIMARY KEY,
            school_name VARCHAR(100),
            school_type VARCHAR(100),
            school_rating CHAR(1),
            open_time CHAR(40),
            close_time CHAR(40)
        );
        """,
        """
        DROP TABLE IF EXISTS neighborhood_school CASCADE;
        CREATE TABLE neighborhood_school (
            neighborhood_id CHAR(200),
            school_id CHAR(200),
            PRIMARY KEY (neighborhood_id, school_id),
            FOREIGN KEY (neighborhood_id) REFERENCES neighborhoods(neighborhood_id),
            FOREIGN KEY (school_id) REFERENCES schools(school_id)
        );
        """,
        """
        DROP TABLE IF EXISTS performance CASCADE;
        CREATE TABLE performance (
            agent_id CHAR(200),
            performance_quarter CHAR(60),
            performance_rating CHAR(10),
            commission_earned NUMERIC(30,2),
            PRIMARY KEY (agent_id, performance_quarter),
            FOREIGN KEY (agent_id) REFERENCES agents(agent_id)
        );
        """,
        """
        DROP TABLE IF EXISTS interest CASCADE;
        CREATE TABLE interest (
            client_id CHAR(200),
            property_id CHAR(200),
            PRIMARY KEY (client_id, property_id),
            FOREIGN KEY (client_id) REFERENCES clients(client_id),
            FOREIGN KEY (property_id) REFERENCES properties(property_id)
        );
        """
    ]

    with conn.cursor() as cur:
        for command in commands:
            cur.execute(command)
        conn.commit()

# Execute the function to create tables
create_tables(conn)

# Close the connection
conn.close()

print("Tables created successfully!")


# # Insert Data

# In[91]:


import pandas as pd
import psycopg
import uuid
import re

# Step 1: Database connection
conn = psycopg.connect(
    host="localhost",
    port="5432",
    dbname="SQL_Project",
    user="postgres",
    password="123"
)

# Step 2: Load Dataset
property_data = pd.read_excel('NY_property.xlsx')

# Step 3: Data Cleaning and Transformation

# Clean Property Data
def clean_property_data(data):
    """Clean and transform property data."""
    data = data.dropna(subset=["price", "homeStatus", "homeType", "livingArea"])
    data = data.rename(columns={
        "price": "listing_price",
        "homeStatus": "property_status_description",
        "homeType": "property_type_description",
        "streetAddress": "street",
        "city": "city",
        "state": "state",
        "zipcode": "zipcode",
        "livingArea": "size",
        "pageViewCount": "comments"
    })
    # Generate a new unique property_id using UUID to avoid collision with other datasets
    data["property_id"] = data.apply(lambda row: str(uuid.uuid4()), axis=1)
    # Clip size to SMALLINT range
    data["size"] = data["size"].clip(lower=-32768, upper=32767)
    # Convert pageViewCount to string for comments
    data["comments"] = data["comments"].fillna("").astype(str)

    # Parse timeOnZillow to get days_on_market
    def parse_time_on_zillow(time_str):
        if pd.isna(time_str):
            return 0  # Default to 0 days if no value is present
        days = 0
        # Extract days if present
        if "day" in time_str:
            match = re.search(r'(\d+)\s*day', time_str)
            if match:
                days += int(match.group(1))
        # Extract hours if present and convert to days
        if "hr" in time_str:
            match = re.search(r'(\d+)\s*hr', time_str)
            if match:
                hours = int(match.group(1))
                days += hours / 24
        return int(days)

    data["days_on_market"] = data["timeOnZillow"].apply(parse_time_on_zillow)

    # Generate neighborhood_id using the last three digits of the zipcode
    data["neighborhood_id"] = data["zipcode"].astype(str).str[-4:]
    
    return data.drop_duplicates()

property_cleaned = clean_property_data(property_data)

# Step 4: Insert Neighborhoods
print("Inserting neighborhoods...")
neighborhoods = property_cleaned[["neighborhood_id"]].drop_duplicates()
neighborhoods["neighborhood_name"] = "Neighborhood_" + neighborhoods["neighborhood_id"]
try:
    insert_data_to_table(neighborhoods, "neighborhoods", conn)
except Exception as e:
    print(f"Failed to insert data into table neighborhoods: {e}")
    conn.rollback()

# Step 5: Insert Property Types
print("Inserting property types...")
property_types = property_cleaned[["property_type_id", "property_type_description"]].drop_duplicates()
try:
    insert_data_to_table(property_types, "property_type", conn)
except Exception as e:
    print(f"Failed to insert data into table property_type: {e}")
    conn.rollback()

# Step 6: Insert Property Statuses
print("Inserting property statuses...")
property_statuses = property_cleaned[["property_status_id", "property_status_description"]].drop_duplicates()
try:
    insert_data_to_table(property_statuses, "property_status", conn)
except Exception as e:
    print(f"Failed to insert data into table property_status: {e}")
    conn.rollback()

# Step 7: Insert Properties
def insert_properties(data, conn):
    """Insert property data into the properties table."""
    properties = data[[
        "property_id", "listing_price", "property_type_id", "property_status_id", 
        "size", "street", "city", "state", "zipcode", "comments", "days_on_market", "neighborhood_id"
    ]]
    try:
        insert_data_to_table(properties, "properties", conn)
    except Exception as e:
        print(f"Failed to insert data into table properties: {e}")
        conn.rollback()

print("Inserting properties...")
insert_properties(property_cleaned, conn)
print("Property data inserted successfully.")

# Insert Data into Tables Function
def insert_data_to_table(data, table_name, conn):
    """Insert data into a specified PostgreSQL table."""
    data = filter_columns(data, table_name)  # Filter columns to match the schema
    with conn.cursor() as cur:
        columns = ', '.join(data.columns)
        placeholders = ', '.join([f"%({col})s" for col in data.columns])
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;"
        try:
            cur.executemany(sql, data.to_dict(orient='records'))
            conn.commit()
            print(f"Data successfully inserted into table: {table_name}")
        except Exception as e:
            print(f"Failed to insert data into table {table_name}: {e}")
            conn.rollback()

# Filter Data Columns to Match Table Schema
def filter_columns(data, table_name):
    """Filter columns to match the schema of the target table."""
    table_columns = {
        "neighborhoods": [
            "neighborhood_id", "neighborhood_name"
        ],
        "properties": [
            "property_id", "listing_price", "property_type_id",
            "property_status_id", "size", "street", "city", "state", "zipcode", "comments", "days_on_market", "neighborhood_id"
        ],
        "property_type": [
            "property_type_id", "property_type_description"
        ],
        "property_status": [
            "property_status_id", "property_status_description"
        ]
    }
    if table_name in table_columns:
        data = data[[col for col in data.columns if col in table_columns[table_name]]]
    return data

# Step 8: Close Database Connection
conn.close()

print("All property data inserted successfully, and database connection closed.")


# In[92]:


import pandas as pd
import psycopg
import uuid

# Step 1: Database connection
conn = psycopg.connect(
    host="localhost",
    port="5432",
    dbname="SQL_Project",
    user="postgres",
    password="123"
)

# Step 2: Load Dataset
school_data = pd.read_csv('School NYC.csv')

# Step 3: Data Cleaning and Transformation

def clean_school_data(data):
    """Clean and transform school data."""
    data = data.dropna(subset=["School Name", "School_District", "City", "zip"])
    data = data.rename(columns={
        "School Name": "school_name",
        "School_District": "school_district_id",
        "City": "city",
        "zip": "zipcode",
        "Address (Full)": "address_full"
    })
    # Generate unique school_id using the last four digits of the zipcode
    data["school_id"] = data["zipcode"].astype(str).str[-4:]  # Use last 4 digits of zipcode as school_id
    # Add default values for school type, rating, and open/close times
    data["school_type"] = "Unknown"
    data["school_rating"] = "0"
    data["open_time"] = "08:00 AM"
    data["close_time"] = "03:00 PM"
    return data.drop_duplicates()

school_cleaned = clean_school_data(school_data)

# Step 4: Insert School Districts

def insert_school_districts(data, conn):
    """Insert unique school districts into the school_district table."""
    school_districts = data[['school_district_id']].drop_duplicates()
    school_districts["school_district_name"] = "District " + school_districts["school_district_id"].astype(str)
    
    try:
        insert_data_to_table(school_districts, "school_district", conn)
    except Exception as e:
        print(f"Failed to insert data into table school_district: {e}")
        conn.rollback()

print("Inserting school districts...")
insert_school_districts(school_cleaned, conn)
print("School district data inserted successfully.")

# Step 5: Generate and Insert Neighborhoods for Schools

def generate_neighborhood_ids_for_schools(data, conn):
    """Assign neighborhood IDs to schools based on matching with neighborhoods table using zip code."""
    with conn.cursor() as cur:
        cur.execute("SELECT neighborhood_id, neighborhood_name FROM neighborhoods;")
        neighborhoods = pd.DataFrame(cur.fetchall(), columns=["neighborhood_id", "neighborhood_name"])
    
    # Match schools with neighborhoods based on city (using neighborhood_name)
    schools_with_neighborhoods = data.merge(neighborhoods, left_on=["city"], right_on=["neighborhood_name"], how="left")
    
    return schools_with_neighborhoods

print("Generating and assigning neighborhood IDs for schools...")
school_cleaned = generate_neighborhood_ids_for_schools(school_cleaned, conn)

# Step 6: Insert Schools

def insert_schools(data, conn):
    """Insert school data into the schools table."""
    schools = data[[
        "school_id", "school_name", "school_type", "school_rating",
        "open_time", "close_time"
    ]]
    try:
        insert_data_to_table(schools, "schools", conn)
    except Exception as e:
        print(f"Failed to insert data into table schools: {e}")
        conn.rollback()

print("Inserting schools...")
insert_schools(school_cleaned, conn)
print("School data inserted successfully.")

# Step 7: Insert Neighborhood Schools

def insert_neighborhood_schools(data, conn):
    """Insert data into the neighborhood_school table, ensuring the neighborhood_id exists in neighborhoods table."""
    neighborhood_schools = data[['school_id', 'neighborhood_id']].drop_duplicates().dropna()
    
    try:
        insert_data_to_table(neighborhood_schools, "neighborhood_school", conn)
    except Exception as e:
        print(f"Failed to insert data into table neighborhood_school: {e}")
        conn.rollback()

print("Inserting neighborhood schools...")
insert_neighborhood_schools(school_cleaned, conn)
print("Neighborhood school data inserted successfully.")

# Step 8: Update Properties Table with School District IDs

def update_properties_with_school_districts(data, conn):
    """Update properties with school district IDs based on neighborhood information."""
    properties_update = data[['school_district_id', 'neighborhood_id']].drop_duplicates().dropna()
    with conn.cursor() as cur:
        for _, row in properties_update.iterrows():
            sql = """
                UPDATE properties
                SET school_district_id = %s
                WHERE neighborhood_id = %s
            """
            try:
                cur.execute(sql, (row['school_district_id'], row['neighborhood_id']))
                conn.commit()
            except Exception as e:
                print(f"Failed to update properties with school district IDs: {e}")
                conn.rollback()

print("Updating properties with school district IDs...")
update_properties_with_school_districts(school_cleaned, conn)
print("Properties updated with school district IDs.")

# Insert Data into Tables Function

def insert_data_to_table(data, table_name, conn):
    """Insert data into a specified PostgreSQL table."""
    data = filter_columns(data, table_name)  # Filter columns to match the schema
    with conn.cursor() as cur:
        columns = ', '.join(data.columns)
        placeholders = ', '.join([f"%({col})s" for col in data.columns])
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;"
        try:
            cur.executemany(sql, data.to_dict(orient='records'))
            conn.commit()
            print(f"Data successfully inserted into table: {table_name}")
        except Exception as e:
            print(f"Failed to insert data into table {table_name}: {e}")
            conn.rollback()

# Filter Data Columns to Match Table Schema

def filter_columns(data, table_name):
    """Filter columns to match the schema of the target table."""
    table_columns = {
        "schools": [
            "school_id", "school_name", "school_type", "school_rating",
            "open_time", "close_time"
        ],
        "neighborhood_school": [
            "school_id", "neighborhood_id"
        ],
        "school_district": [
            "school_district_id", "school_district_name"
        ]
    }
    if table_name in table_columns:
        data = data[[col for col in data.columns if col in table_columns[table_name]]]
    return data

# Step 9: Close Database Connection
conn.close()

print("All school data inserted successfully, and database connection closed.")


# In[93]:


import pandas as pd
import psycopg
import random

# Step 1: Database connection
conn = psycopg.connect(
    host="localhost",
    port="5432",
    dbname="SQL_Project",
    user="postgres",
    password="123"
)

# Step 2: Load Neighborhood IDs and School IDs
with conn.cursor() as cur:
    # Fetch all neighborhood IDs from the neighborhoods table
    cur.execute("SELECT neighborhood_id FROM neighborhoods;")
    neighborhoods = [row[0] for row in cur.fetchall()]
    
    # Fetch all school IDs from the schools table
    cur.execute("SELECT school_id FROM schools;")
    schools = [row[0] for row in cur.fetchall()]

# Step 3: Randomly Assign Neighborhoods to Schools
random.shuffle(neighborhoods)  # Shuffle neighborhoods to ensure random pairing

# Step 4: Create a DataFrame to Insert into Neighborhood_School Table
# If there are more schools than neighborhoods, reuse neighborhoods
assignments = [(school, random.choice(neighborhoods)) for school in schools]

neighborhood_school_df = pd.DataFrame(assignments, columns=['school_id', 'neighborhood_id'])

# Step 5: Insert Data into Neighborhood_School Table
def insert_data_to_table(data, table_name, conn):
    """Insert data into a specified PostgreSQL table."""
    with conn.cursor() as cur:
        columns = ', '.join(data.columns)
        placeholders = ', '.join([f"%({col})s" for col in data.columns])
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;"
        try:
            cur.executemany(sql, data.to_dict(orient='records'))
            conn.commit()
            print(f"Data successfully inserted into table: {table_name}")
        except Exception as e:
            print(f"Failed to insert data into table {table_name}: {e}")
            conn.rollback()

print("Inserting neighborhood-school assignments...")
insert_data_to_table(neighborhood_school_df, "neighborhood_school", conn)
print("Neighborhood-school data inserted successfully.")

# Step 6: Close Database Connection
conn.close()

print("Database connection closed.")


# In[84]:


import pandas as pd
import psycopg

# Step 1: Database connection
conn = psycopg.connect(
    host="localhost",
    port="5432",
    dbname="SQL_Project",
    user="postgres",
    password="123"
)

# Step 2: Load School District IDs and Property IDs
with conn.cursor() as cur:
    # Fetch all school district IDs from the school_district table
    cur.execute("SELECT school_district_id FROM school_district;")
    school_districts = [row[0] for row in cur.fetchall()]
    
    # Fetch all property IDs from the properties table
    cur.execute("SELECT property_id FROM properties;")
    properties = [row[0] for row in cur.fetchall()]

# Step 3: Assign School Districts to Properties in a Repeating Pattern
school_district_ids_repeated = (school_districts * (len(properties) // len(school_districts) + 1))[:len(properties)]
assignments = list(zip(properties, school_district_ids_repeated))

# Step 4: Create a DataFrame to Insert into Properties Table
property_school_district_df = pd.DataFrame(assignments, columns=['property_id', 'school_district_id'])

# Step 5: Update Data in Properties Table
def update_properties_with_school_district(data, conn):
    """Update properties table with school district IDs."""
    with conn.cursor() as cur:
        for _, row in data.iterrows():
            sql = """
                UPDATE properties
                SET school_district_id = %s
                WHERE property_id = %s;
            """
            try:
                cur.execute(sql, (row['school_district_id'], row['property_id']))
                conn.commit()
            except Exception as e:
                print(f"Failed to update property {row['property_id']} with school district ID: {e}")
                conn.rollback()

print("Updating properties with school district IDs...")
update_properties_with_school_district(property_school_district_df, conn)
print("Properties updated with school district IDs.")

# Step 6: Close Database Connection
conn.close()

print("Database connection closed.")


# # Agents

# In[94]:


import psycopg
import pandas as pd
import uuid
from faker import Faker
import random

# Initialize Faker for generating sample data
fake = Faker()

# Database connection configuration
conn = psycopg.connect(
    host="localhost",
    port="5432",
    dbname="SQL_Project",
    user="postgres",
    password="123"
)

# Step 1: Load Office IDs from offices table
with conn.cursor() as cur:
    cur.execute("SELECT office_id FROM offices;")
    office_ids = [row[0] for row in cur.fetchall()]

# Step 1.1: Insert Offices if no records are found
if not office_ids:
    print("No office IDs found. Inserting sample office records...")
    num_offices = 5
    office_data = []
    for _ in range(num_offices):
        office_id = str(uuid.uuid4())
        office_name = fake.company()
        street_address = fake.street_address()
        city = fake.city()
        state = fake.state_abbr()
        zipcode = fake.zipcode()
        office_data.append([office_id, office_name, street_address, city, state, zipcode])
    
    # Insert office data into offices table
    df_offices = pd.DataFrame(office_data, columns=[
        'office_id', 'office_name', 'street_address', 'city', 'state', 'zipcode'
    ])
    with conn.cursor() as cur:
        columns = ', '.join(df_offices.columns)
        placeholders = ', '.join([f'%({col})s' for col in df_offices.columns])
        sql = f"INSERT INTO offices ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;"
        try:
            cur.executemany(sql, df_offices.to_dict(orient='records'))
            conn.commit()
            print("Sample office data successfully inserted.")
        except Exception as e:
            print(f"Failed to insert data into table offices: {e}")
            conn.rollback()
    
    # Reload office_ids after inserting sample offices
    with conn.cursor() as cur:
        cur.execute("SELECT office_id FROM offices;")
        office_ids = [row[0] for row in cur.fetchall()]

# Step 2: Generate Sample Data for Agents Table
def generate_agent_data(num_records, office_ids):
    agents_data = []
    for _ in range(num_records):
        agent_id = str(uuid.uuid4())
        office_id = random.choice(office_ids)  # Randomly assign an existing office_id
        first_name = fake.first_name()
        last_name = fake.last_name()
        dob = fake.date_of_birth(minimum_age=25, maximum_age=65)
        hire_date = fake.date_between(start_date='-10y', end_date='today')
        email = fake.email()
        phone_number = fake.phone_number()

        agents_data.append([
            agent_id, office_id, first_name, last_name, dob, hire_date, email, phone_number
        ])
    return agents_data

# Generate 20 sample agent records
num_agents = 20
agents_data = generate_agent_data(num_agents, office_ids)

# Step 3: Insert Agents Data into agents Table
def insert_agents_data(data, conn):
    df = pd.DataFrame(data, columns=[
        'agent_id', 'office_id', 'first_name', 'last_name', 'dob', 'hire_date', 'email', 'phone_number'
    ])
    with conn.cursor() as cur:
        columns = ', '.join(df.columns)
        placeholders = ', '.join([f'%({col})s' for col in df.columns])
        sql = f"INSERT INTO agents ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;"
        try:
            cur.executemany(sql, df.to_dict(orient='records'))
            conn.commit()
            print("Data successfully inserted into table: agents")
        except Exception as e:
            print(f"Failed to insert data into table agents: {e}")
            conn.rollback()

print("Inserting agents data...")
insert_agents_data(agents_data, conn)

# Step 4: Close Database Connection
conn.close()
print("Database connection closed.")


# # Clients

# In[95]:


import psycopg
import pandas as pd
import uuid
from faker import Faker

# Initialize Faker for generating sample data
fake = Faker()

# Database connection configuration
conn = psycopg.connect(
    host="localhost",
    port="5432",
    dbname="SQL_Project",
    user="postgres",
    password="123"
)

# Step 1: Generate Sample Data for Clients Table
def generate_client_data(num_records):
    clients_data = []
    for _ in range(num_records):
        client_id = str(uuid.uuid4())
        client_name = fake.name()
        street = fake.street_address()
        city = fake.city()
        state = fake.state_abbr()
        zipcode = fake.zipcode()
        email = fake.email()
        phone_number = fake.phone_number()
        comments = fake.sentence(nb_words=10)  # Adding a brief comment

        clients_data.append([
            client_id, client_name, street, city, state, zipcode, email, phone_number, comments
        ])
    return clients_data

# Generate 20 sample client records
num_clients = 20
clients_data = generate_client_data(num_clients)

# Step 2: Insert Clients Data into clients Table
def insert_clients_data(data, conn):
    df = pd.DataFrame(data, columns=[
        'client_id', 'client_name', 'street', 'city', 'state', 'zipcode', 'email', 'phone_number', 'comments'
    ])
    with conn.cursor() as cur:
        columns = ', '.join(df.columns)
        placeholders = ', '.join([f'%({col})s' for col in df.columns])
        sql = f"INSERT INTO clients ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;"
        try:
            cur.executemany(sql, df.to_dict(orient='records'))
            conn.commit()
            print("Data successfully inserted into table: clients")
        except Exception as e:
            print(f"Failed to insert data into table clients: {e}")
            conn.rollback()

print("Inserting clients data...")
insert_clients_data(clients_data, conn)

# Step 3: Close Database Connection
conn.close()
print("Database connection closed.")


# # Office

# In[96]:


import psycopg
import pandas as pd
import uuid
from faker import Faker

# Initialize Faker for generating sample data
fake = Faker()

# Database connection configuration
conn = psycopg.connect(
    host="localhost",
    port="5432",
    dbname="SQL_Project",
    user="postgres",
    password="123"
)

# Step 1: Generate Sample Data for Offices Table
def generate_office_data(num_records):
    office_data = []
    for _ in range(num_records):
        office_id = str(uuid.uuid4())
        office_name = fake.company() + " Office"
        street_address = fake.street_address()
        city = fake.city()
        state = fake.state_abbr()
        zipcode = fake.zipcode()

        office_data.append([office_id, office_name, street_address, city, state, zipcode])
    return office_data

# Generate 10 sample office records
num_offices = 10
offices_data = generate_office_data(num_offices)

# Step 2: Insert Offices Data into offices Table
def insert_offices_data(data, conn):
    df = pd.DataFrame(data, columns=[
        'office_id', 'office_name', 'street_address', 'city', 'state', 'zipcode'
    ])
    with conn.cursor() as cur:
        columns = ', '.join(df.columns)
        placeholders = ', '.join([f'%({col})s' for col in df.columns])
        sql = f"INSERT INTO offices ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;"
        try:
            cur.executemany(sql, df.to_dict(orient='records'))
            conn.commit()
            print("Data successfully inserted into table: offices")
        except Exception as e:
            print(f"Failed to insert data into table offices: {e}")
            conn.rollback()

print("Inserting offices data...")
insert_offices_data(offices_data, conn)

# Step 3: Close Database Connection
conn.close()
print("Database connection closed.")


# # Amenities

# In[98]:


import psycopg
import pandas as pd
import uuid
from faker import Faker

# Initialize Faker for generating sample data
fake = Faker()

# Database connection configuration
conn = psycopg.connect(
    host="localhost",
    port="5432",
    dbname="SQL_Project",
    user="postgres",
    password="123"
)

# Step 1: Generate Sample Data for Amenities Table
def generate_amenity_data(num_records):
    amenity_data = []
    amenities_list = [
        "Swimming Pool", "Gym", "Parking", "Elevator", "Security System", 
        "Garden", "Playground", "Wi-Fi", "Fireplace", "Laundry"
    ]
    for i in range(num_records):
        amenity_id = str(uuid.uuid4())
        amenity_name = fake.random_element(elements=amenities_list)

        amenity_data.append([amenity_id, amenity_name])
    return amenity_data

# Generate 10 sample amenity records
num_amenities = 10
amenities_data = generate_amenity_data(num_amenities)

# Step 2: Insert Amenities Data into amenities Table
def insert_amenities_data(data, conn):
    df = pd.DataFrame(data, columns=['amenity_id', 'amenity_name'])
    with conn.cursor() as cur:
        columns = ', '.join(df.columns)
        placeholders = ', '.join([f'%({col})s' for col in df.columns])
        sql = f"INSERT INTO amenities ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;"
        try:
            cur.executemany(sql, df.to_dict(orient='records'))
            conn.commit()
            print("Data successfully inserted into table: amenities")
        except Exception as e:
            print(f"Failed to insert data into table amenities: {e}")
            conn.rollback()

print("Inserting amenities data...")
insert_amenities_data(amenities_data, conn)

# Step 3: Close Database Connection
conn.close()
print("Database connection closed.")


# # Appointments

# In[99]:


import psycopg
import pandas as pd
import uuid
from faker import Faker
import random

# Initialize Faker for generating sample data
fake = Faker()

# Database connection configuration
conn = psycopg.connect(
    host="localhost",
    port="5432",
    dbname="SQL_Project",
    user="postgres",
    password="123"
)

# Step 1: Load Agent IDs, Client IDs, and Property IDs
with conn.cursor() as cur:
    cur.execute("SELECT agent_id FROM agents;")
    agent_ids = [row[0] for row in cur.fetchall()]
    
    cur.execute("SELECT client_id FROM clients;")
    client_ids = [row[0] for row in cur.fetchall()]
    
    cur.execute("SELECT property_id FROM properties;")
    property_ids = [row[0] for row in cur.fetchall()]

# Step 2: Generate Sample Data for Appointments Table
def generate_appointment_data(num_records, agent_ids, client_ids, property_ids):
    appointments_data = []
    for _ in range(num_records):
        agent_id = random.choice(agent_ids)
        client_id = random.choice(client_ids)
        property_id = random.choice(property_ids)
        appointment_time = fake.date_time_between(start_date='-1y', end_date='now')
        comments = fake.sentence(nb_words=10)

        appointments_data.append([agent_id, client_id, property_id, appointment_time, comments])
    return appointments_data

# Generate 20 sample appointment records
num_appointments = 20
appointments_data = generate_appointment_data(num_appointments, agent_ids, client_ids, property_ids)

# Step 3: Insert Appointments Data into appointments Table
def insert_appointments_data(data, conn):
    df = pd.DataFrame(data, columns=[
        'agent_id', 'client_id', 'property_id', 'appointment_time', 'comments'
    ])
    with conn.cursor() as cur:
        columns = ', '.join(df.columns)
        placeholders = ', '.join([f'%({col})s' for col in df.columns])
        sql = f"INSERT INTO appointments ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;"
        try:
            cur.executemany(sql, df.to_dict(orient='records'))
            conn.commit()
            print("Data successfully inserted into table: appointments")
        except Exception as e:
            print(f"Failed to insert data into table appointments: {e}")
            conn.rollback()

print("Inserting appointments data...")
insert_appointments_data(appointments_data, conn)

# Step 4: Close Database Connection
conn.close()
print("Database connection closed.")


# # Expenses

# In[100]:


import psycopg
import pandas as pd
import uuid
from faker import Faker
import random

# Initialize Faker for generating sample data
fake = Faker()

# Database connection configuration
conn = psycopg.connect(
    host="localhost",
    port="5432",
    dbname="SQL_Project",
    user="postgres",
    password="123"
)

# Step 1: Load Office IDs and Agent IDs
with conn.cursor() as cur:
    cur.execute("SELECT office_id FROM offices;")
    office_ids = [row[0] for row in cur.fetchall()]
    
    cur.execute("SELECT agent_id FROM agents;")
    agent_ids = [row[0] for row in cur.fetchall()]

# Step 2: Generate Sample Data for Expenses Table
def generate_expense_data(num_records, office_ids, agent_ids):
    expenses_data = []
    for _ in range(num_records):
        expense_id = str(uuid.uuid4())
        office_id = random.choice(office_ids)
        agent_id = random.choice(agent_ids)
        expense_item = random.randint(1, 10)  # Assuming expense_item is a category identifier from 1 to 10
        quantity = round(random.uniform(1, 20), 2)
        unit_price = round(random.uniform(10, 1000), 2)
        total_expensed_amount = round(quantity * unit_price, 2)

        expenses_data.append([
            expense_id, office_id, agent_id, expense_item, quantity, unit_price, total_expensed_amount
        ])
    return expenses_data

# Generate 20 sample expense records
num_expenses = 20
expenses_data = generate_expense_data(num_expenses, office_ids, agent_ids)

# Step 3: Insert Expenses Data into expenses Table
def insert_expenses_data(data, conn):
    df = pd.DataFrame(data, columns=[
        'expense_id', 'office_id', 'agent_id', 'expense_item', 'quantity', 'unit_price', 'total_expensed_amount'
    ])
    with conn.cursor() as cur:
        columns = ', '.join(df.columns)
        placeholders = ', '.join([f'%({col})s' for col in df.columns])
        sql = f"INSERT INTO expenses ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;"
        try:
            cur.executemany(sql, df.to_dict(orient='records'))
            conn.commit()
            print("Data successfully inserted into table: expenses")
        except Exception as e:
            print(f"Failed to insert data into table expenses: {e}")
            conn.rollback()

print("Inserting expenses data...")
insert_expenses_data(expenses_data, conn)

# Step 4: Close Database Connection
conn.close()
print("Database connection closed.")


# # Interest

# In[101]:


import psycopg
import pandas as pd
import uuid
from faker import Faker
import random

# Initialize Faker for generating sample data
fake = Faker()

# Database connection configuration
conn = psycopg.connect(
    host="localhost",
    port="5432",
    dbname="SQL_Project",
    user="postgres",
    password="123"
)

# Step 1: Load Client IDs and Property IDs
with conn.cursor() as cur:
    cur.execute("SELECT client_id FROM clients;")
    client_ids = [row[0] for row in cur.fetchall()]

    cur.execute("SELECT property_id FROM properties;")
    property_ids = [row[0] for row in cur.fetchall()]

# Step 2: Generate Sample Data for Interest Table
def generate_interest_data(num_records, client_ids, property_ids):
    interests_data = []
    for _ in range(num_records):
        client_id = random.choice(client_ids)
        property_id = random.choice(property_ids)

        # To avoid duplicate entries
        if (client_id, property_id) not in interests_data:
            interests_data.append([client_id, property_id])
    
    return interests_data

# Generate 50 sample interest records
num_interests = 50
interests_data = generate_interest_data(num_interests, client_ids, property_ids)

# Step 3: Insert Interests Data into interest Table
def insert_interest_data(data, conn):
    df = pd.DataFrame(data, columns=['client_id', 'property_id'])
    with conn.cursor() as cur:
        columns = ', '.join(df.columns)
        placeholders = ', '.join([f'%({col})s' for col in df.columns])
        sql = f"INSERT INTO interest ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;"
        try:
            cur.executemany(sql, df.to_dict(orient='records'))
            conn.commit()
            print("Data successfully inserted into table: interest")
        except Exception as e:
            print(f"Failed to insert data into table interest: {e}")
            conn.rollback()

print("Inserting interest data...")
insert_interest_data(interests_data, conn)

# Step 4: Close Database Connection
conn.close()
print("Database connection closed.")


# # Performance

# In[102]:


import psycopg
import pandas as pd
import uuid
from faker import Faker
import random

# Initialize Faker for generating sample data
fake = Faker()

# Database connection configuration
conn = psycopg.connect(
    host="localhost",
    port="5432",
    dbname="SQL_Project",
    user="postgres",
    password="123"
)

# Step 1: Load Agent IDs from agents table
with conn.cursor() as cur:
    cur.execute("SELECT agent_id FROM agents;")
    agent_ids = [row[0] for row in cur.fetchall()]

# Step 2: Generate Sample Data for Performance Table
def generate_performance_data(agent_ids):
    performance_data = []
    performance_quarters = ['Q1 2023', 'Q2 2023', 'Q3 2023', 'Q4 2023']
    ratings = ['A', 'B', 'C', 'D']
    
    for agent_id in agent_ids:
        for quarter in performance_quarters:
            performance_rating = random.choice(ratings)
            commission_earned = round(random.uniform(5000, 50000), 2)
            performance_data.append([agent_id, quarter, performance_rating, commission_earned])
    
    return performance_data

# Generate performance data for each agent
performance_data = generate_performance_data(agent_ids)

# Step 3: Insert Performance Data into performance Table
def insert_performance_data(data, conn):
    df = pd.DataFrame(data, columns=['agent_id', 'performance_quarter', 'performance_rating', 'commission_earned'])
    with conn.cursor() as cur:
        columns = ', '.join(df.columns)
        placeholders = ', '.join([f'%({col})s' for col in df.columns])
        sql = f"INSERT INTO performance ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;"
        try:
            cur.executemany(sql, df.to_dict(orient='records'))
            conn.commit()
            print("Data successfully inserted into table: performance")
        except Exception as e:
            print(f"Failed to insert data into table performance: {e}")
            conn.rollback()

print("Inserting performance data...")
insert_performance_data(performance_data, conn)

# Step 4: Close Database Connection
conn.close()
print("Database connection closed.")


# # Property_Amenity

# In[103]:


import psycopg
import pandas as pd
import random

# Database connection configuration
conn = psycopg.connect(
    host="localhost",
    port="5432",
    dbname="SQL_Project",
    user="postgres",
    password="123"
)

# Step 1: Load Property IDs and Amenity IDs
with conn.cursor() as cur:
    cur.execute("SELECT property_id FROM properties;")
    property_ids = [row[0] for row in cur.fetchall()]
    
    cur.execute("SELECT amenity_id FROM amenities;")
    amenity_ids = [row[0] for row in cur.fetchall()]

# Step 2: Generate Sample Data for Property_Amenity Table
def generate_property_amenity_data(property_ids, amenity_ids, num_entries=50):
    property_amenity_data = []
    for _ in range(num_entries):
        property_id = random.choice(property_ids)
        amenity_id = random.choice(amenity_ids)
        property_amenity_data.append([amenity_id, property_id])
    return property_amenity_data

# Generate sample data for property_amenity table
num_entries = 50  # Number of entries to generate
property_amenity_data = generate_property_amenity_data(property_ids, amenity_ids, num_entries)

# Step 3: Insert Property Amenity Data into property_amenity Table
def insert_property_amenity_data(data, conn):
    df = pd.DataFrame(data, columns=['amenity_id', 'property_id'])
    with conn.cursor() as cur:
        columns = ', '.join(df.columns)
        placeholders = ', '.join([f'%({col})s' for col in df.columns])
        sql = f"INSERT INTO property_amenity ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;"
        try:
            cur.executemany(sql, df.to_dict(orient='records'))
            conn.commit()
            print("Data successfully inserted into table: property_amenity")
        except Exception as e:
            print(f"Failed to insert data into table property_amenity: {e}")
            conn.rollback()

print("Inserting property_amenity data...")
insert_property_amenity_data(property_amenity_data, conn)

# Step 4: Close Database Connection
conn.close()
print("Database connection closed.")


# # Transaction

# In[104]:


import psycopg
import pandas as pd
import uuid
import random
from faker import Faker

# Initialize Faker for generating sample data
fake = Faker()

# Database connection configuration
conn = psycopg.connect(
    host="localhost",
    port="5432",
    dbname="SQL_Project",
    user="postgres",
    password="123"
)

# Step 1: Load Client IDs, Property IDs, and Agent IDs
with conn.cursor() as cur:
    cur.execute("SELECT client_id FROM clients;")
    client_ids = [row[0] for row in cur.fetchall()]
    
    cur.execute("SELECT property_id FROM properties;")
    property_ids = [row[0] for row in cur.fetchall()]
    
    cur.execute("SELECT agent_id FROM agents;")
    agent_ids = [row[0] for row in cur.fetchall()]

# Step 2: Generate Sample Data for Transactions Table
def generate_transaction_data(client_ids, property_ids, agent_ids, num_records=30):
    transactions_data = []
    for _ in range(num_records):
        client_id = random.choice(client_ids)
        property_id = random.choice(property_ids)
        transaction_date = fake.date_time_between(start_date='-2y', end_date='now')
        agent_id = random.choice(agent_ids)
        transaction_amount = round(random.uniform(100000, 1000000), 2)

        transactions_data.append([client_id, property_id, transaction_date, agent_id, transaction_amount])
    return transactions_data

# Generate 30 sample transaction records
num_transactions = 30
transactions_data = generate_transaction_data(client_ids, property_ids, agent_ids, num_transactions)

# Step 3: Insert Transactions Data into transactions Table
def insert_transactions_data(data, conn):
    df = pd.DataFrame(data, columns=[
        'client_id', 'property_id', 'transaction_date', 'agent_id', 'transaction_amount'
    ])
    with conn.cursor() as cur:
        columns = ', '.join(df.columns)
        placeholders = ', '.join([f'%({col})s' for col in df.columns])
        sql = f"INSERT INTO transactions ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;"
        try:
            cur.executemany(sql, df.to_dict(orient='records'))
            conn.commit()
            print("Data successfully inserted into table: transactions")
        except Exception as e:
            print(f"Failed to insert data into table transactions: {e}")
            conn.rollback()

print("Inserting transactions data...")
insert_transactions_data(transactions_data, conn)

# Step 4: Close Database Connection
conn.close()
print("Database connection closed.")


# In[ ]:




