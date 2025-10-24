import mysql.connector
from mysql.connector import Error
import random
from datetime import date, timedelta

# --- Mock Data for Realism ---
FIRST_NAMES = ['John', 'Jane', 'Alex', 'Emily', 'Chris', 'Katie', 'Michael', 'Sarah', 'David', 'Laura', 'Robert', 'Emma']
LAST_NAMES = ['Smith', 'Doe', 'Johnson', 'Brown', 'Davis', 'Miller', 'Wilson', 'Moore', 'Taylor', 'Anderson', 'Thomas']
DOMAINS = ['example.com', 'mail.com', 'web.net', 'personal.org']
CITIES = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego']
STREETS = ['Main St', 'Oak Ave', 'Pine St', 'Maple Dr', 'Elm St', 'Cedar Ln']
# -----------------------------

def create_connection():
    """ Creates a database connection. """
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="MySQL_Student123", # Change as needed
            database="p002" # Change as needed
        )
        if connection.is_connected():
            print("Connection successful")
        return connection
    except Error as e:
        print(f"Error: {e}")
        return None

def generate_random_client():
    """Generates a tuple for a new client."""
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    return (first, last)

def generate_random_client_info(customer_id):
    """Generates info for a specific client ID."""
    # Generate a random date of birth (18-70 years old)
    today = date.today()
    start_date = today - timedelta(days=70*365)
    end_date = today - timedelta(days=18*365)
    random_days = random.randint(0, (end_date - start_date).days)
    dob = start_date + timedelta(days=random_days)
    
    address = f"{random.randint(1, 9999)} {random.choice(STREETS)}, {random.choice(CITIES)}"
    phone = f"{random.randint(1000000000, 9999999999)}"
    
    # Generate a unique email based on customerId to avoid collisions
    email = f"user.{customer_id}.{random.randint(100,999)}@{random.choice(DOMAINS)}"
    
    return (customer_id, dob, address, phone, email)

def generate_random_account(customer_id, account_id):
    """Generates a bank account for a client ID and account ID."""
    balance = round(random.uniform(50.00, 500000.00), 2)
    # Skew status towards 'Active'
    status = random.choices(["Active", "Closed"], weights=[0.95, 0.05], k=1)[0]
    
    # Generate a random open date (within the last 15 years)
    today = date.today()
    start_date = today - timedelta(days=15*365)
    random_days = random.randint(0, (today - start_date).days)
    open_date = start_date + timedelta(days=random_days)
    
    return (account_id, customer_id, balance, status, open_date)

def insert_bulk_data(connection, n=300000, batch_size=5000):
    """
    Inserts 'n' clients and their related data in batches.
    Each client will get 1 info record and 1-3 account records.
    """
    cursor = connection.cursor()
    
    # SQL INSERT statements
    sql_client = "INSERT INTO clients (first_name, last_name) VALUES (%s, %s)"
    sql_client_info = """
        INSERT INTO clients_info 
        (customerId, date_of_birth, address, phone_number, email) 
        VALUES (%s, %s, %s, %s, %s)
    """
    sql_account = """
        INSERT INTO accounts 
        (accountId, customerId, balance, status, open_date) 
        VALUES (%s, %s, %s, %s, %s)
    """

    # Start accountId from a high number to avoid collisions
    next_account_id = 1000001
    total_clients_inserted = 0
    total_accounts_inserted = 0

    print(f"Starting bulk insert of {n} clients in batches of {batch_size}...")

    for start in range(0, n, batch_size):
        # --- 1. Generate and Insert Clients ---
        clients_data = [generate_random_client() for _ in range(batch_size)]
        cursor.executemany(sql_client, clients_data)
        
        # --- 2. Get the new Client IDs ---
        # We get the *first* ID inserted in this batch
        first_client_id = cursor.lastrowid
        rows_inserted = cursor.rowcount
        # Create a list of all IDs just inserted (e.g., 1001, 1002, ... 2000)
        customer_id_batch = list(range(first_client_id, first_client_id + rows_inserted))
        
        # --- 3. Generate and Insert Client Info ---
        clients_info_data = [generate_random_client_info(cid) for cid in customer_id_batch]
        cursor.executemany(sql_client_info, clients_info_data)

        # --- 4. Generate and Insert Accounts ---
        accounts_data = []
        for cid in customer_id_batch:
            # Give each client 1 to 3 accounts
            num_accounts = random.randint(1, 3) 
            for _ in range(num_accounts):
                accounts_data.append(generate_random_account(cid, next_account_id))
                next_account_id += 1 # Increment the unique account ID
        
        cursor.executemany(sql_account, accounts_data)

        # --- 5. Commit the Transaction ---
        # Commit all inserts for this batch (clients, info, accounts) at once
        connection.commit()

        total_clients_inserted += rows_inserted
        total_accounts_inserted += len(accounts_data)
        
        print(f"  Batch {start//batch_size + 1}/{n//batch_size}:")
        print(f"    - Inserted {rows_inserted} clients (IDs: {first_client_id} to {first_client_id + rows_inserted - 1})")
        print(f"    - Inserted {len(clients_info_data)} client_info records")
        print(f"    - Inserted {len(accounts_data)} account records")
        print(f"  Total Clients: {total_clients_inserted}, Total Accounts: {total_accounts_inserted}")
        print("---")

    print("All records inserted successfully.")
    print(f"Final Totals: {total_clients_inserted} Clients, {total_accounts_inserted} Accounts.")


if __name__ == "__main__":
    conn = create_connection()
    if conn:
        # WARNING: n=5,000,000 will create ~5M clients, 5M client_info,
        # and ~10-15M accounts. This will take a very long time and
        # require significant disk space.
        #
        # Starting with 10,000 clients (in batches of 1000) for testing.
        insert_bulk_data(conn, n=300000, batch_size=5000)
        
        # Uncomment the line below for your full 5M load:
        # insert_bulk_data(conn, n=5000000, batch_size=5000)
        
        conn.close()
        print("Connection closed.")