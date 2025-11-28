"""
Author: Laurentiu Codreanu (C24387041)
Title : Data Centric Programming Assignment 2025
Description : Coding a program which parses abc files into our sql tables, inserts the data into the table and allows us to physically interact with it using a menu
"""

import os
import mysql.connector
import pandas as pd
import re

# Constants
ABC_ROOT = "abc_books"


def connectToDB():
    """Connect to MySQL database"""
    # Create connection to local MySQL server
    # Returns: connection object that we can use to run queries
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='',
        database='abc_books'
    )


# ============================================================================
# PART 1: FILE LOADING & PARSING
# ============================================================================

def find_abc_files():
    """
    Find all .abc files in the abc_books directory
    Returns: list of tuples [(filepath, book_number), ...]
    """
    # Use a LIST to store file paths because we need to iterate through them later
    # Lists are good when you need an ordered collection you can loop through
    abc_files = []
    
    # Check if the abc_books folder exists
    if not os.path.exists(ABC_ROOT):
        print(f"Error: Directory '{ABC_ROOT}' not found!")
        return abc_files
    
    # Look through each folder in abc_books (these are our book folders)
    for folder in os.listdir(ABC_ROOT):
        folder_path = os.path.join(ABC_ROOT, folder)
        
        # Check if it's a directory with a number as name (book number)
        if os.path.isdir(folder_path) and folder.isdigit():
            book_number = folder  # The folder name IS the book number
            print(f"Found book: {book_number}")
            
            # Find all .abc files in this book folder
            for file in os.listdir(folder_path):
                if file.endswith('.abc'):
                    file_path = os.path.join(folder_path, file)
                    # Store as tuple: (file path, book number)
                    abc_files.append((file_path, book_number))
                    print(f"  Found: {file}")
    
    return abc_files


def parse_abc_file(filepath, book_number):
    """
    Parse an ABC file and return a list of tunes
    
    HOW PARSING WORKS:
    1. Read the file line by line
    2. When we find 'X:' it means a NEW TUNE starts
    3. We collect all information (T:, K:, R:, etc.) for that tune
    4. When we find the NEXT 'X:' we save the current tune and start a new one
    5. Continue until end of file
    
    Args:
        filepath: path to the .abc file
        book_number: which book this file belongs to
    
    Returns: list of dictionaries, each dictionary is one tune
    """
    # Use a LIST to store multiple tunes (ordered collection)
    tunes = []
    
    # Use a DICTIONARY to store one tune's data (key-value pairs like 'title': 'The First of May')
    # Dictionaries are perfect here because each tune has multiple properties (title, key, rhythm, etc.)
    current_tune = {}
    
    try:
        # Open and read the file
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                line = line.strip()  # Remove whitespace from start and end
                
                # Skip empty lines and comments (comments start with %)
                if not line or line.startswith('%'):
                    continue
                
                # X: starts a NEW TUNE
                if line.startswith('X:'):
                    # If we already have a tune stored, save it to our list
                    if current_tune:
                        tunes.append(current_tune)
                    
                    # Start a brand new tune dictionary
                    # Initialize all fields to empty strings
                    current_tune = {
                        'reference': line[2:].strip(),  # Get everything after 'X:'
                        'title': '',
                        'alt_title': '',  # Alternative title (if tune has multiple names)
                        'book_number': book_number,
                        'key': '',
                        'rhythm': '',
                        'notes': ''
                    }
                
                # If we're currently building a tune, collect its information
                elif current_tune:
                    # T: is the title
                    if line.startswith('T:'):
                        # If title already exists, this is an alternative title
                        if current_tune['title']:
                            current_tune['alt_title'] = line[2:].strip()
                        else:
                            current_tune['title'] = line[2:].strip()
                    
                    # K: is the key signature (e.g., G major, D minor)
                    elif line.startswith('K:'):
                        current_tune['key'] = line[2:].strip()
                    
                    # R: is the rhythm/type (e.g., jig, reel, hornpipe)
                    elif line.startswith('R:'):
                        current_tune['rhythm'] = line[2:].strip()
                    
                    # If line doesn't start with a letter and colon, it's music notation
                    elif not re.match(r'^[A-Z]:', line):
                        current_tune['notes'] += line + ' '
        
        # Don't forget the last tune in the file!
        if current_tune:
            tunes.append(current_tune)
        
        print(f"  Parsed {len(tunes)} tunes")
            
    except Exception as e:
        print(f"Error parsing {filepath}: {e}")
    
    return tunes


def insert_tunes_to_db(tunes, table_name='music'):
    """
    Insert tunes into the database
    
    SQL STATEMENT: INSERT INTO music (columns) VALUES (values)
    This adds new rows to the music table
    
    Args:
        tunes: list of tune dictionaries
        table_name: name of database table (default: 'music')
    
    Returns: number of tunes successfully inserted
    """
    if not tunes:
        print("No tunes to insert")
        return 0
    
    # Connect to database
    conn = connectToDB()
    cursor = conn.cursor()  # Cursor is used to execute SQL statements
    
    # SQL INSERT statement
    # This tells MySQL which columns to fill and uses %s as placeholders for values
    query = """
        INSERT INTO {} (book_number, tune_id, title, alt_title, tune_type, key_signature, notation)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """.format(table_name)
    
    inserted = 0  # Counter for successful inserts
    
    # Loop through each tune and insert it
    for tune in tunes:
        # Create a tuple of values in the same order as the columns above
        # .get() safely gets dictionary values, returns empty string if key doesn't exist
        values = (
            tune.get('book_number', ''),
            tune.get('reference', ''),
            tune.get('title', ''),
            tune.get('alt_title', ''),
            tune.get('rhythm', ''),
            tune.get('key', ''),
            tune.get('notes', '')
        )
        
        try:
            # Execute the INSERT query with these values
            # MySQL replaces each %s with the corresponding value
            cursor.execute(query, values)
            inserted += 1
        except mysql.connector.Error as err:
            print(f"Error inserting '{tune.get('title', 'Unknown')}': {err}")
    
    # Commit saves all changes to the database
    conn.commit()
    conn.close()
    
    print(f"Inserted {inserted} tunes into database")
    return inserted


# ============================================================================
# PART 2: DATA LOADING WITH PANDAS
# ============================================================================

def load_data_to_dataframe(table_name='music'):
    """
    Load tunes from database into a pandas DataFrame
    
    SQL STATEMENT: SELECT * FROM music
    This retrieves ALL rows and ALL columns from the music table
    Returns: a DataFrame with all the data
    
    Args:
        table_name: name of table to load (default: 'music')
    
    Returns: pandas DataFrame with all tune data
    """
    conn = connectToDB()
    
    query = f"SELECT * FROM {table_name}"
    
    # pd.read_sql runs the query and puts results in a DataFrame
    df = pd.read_sql(query, conn)
    conn.close()
    
    print(f"Loaded {len(df)} tunes into DataFrame")
    return df


# ============================================================================
# PART 3: INTERACTIVE MENU
# ============================================================================

def display_menu():
    """
    Show the main menu to the user
    This is the UI - simple text-based menu using print statements
    """
    print("\n" + "="*50)
    print("ABC MUSIC DATABASE MENU")
    print("="*50)
    print("1. Load ABC files into database")
    print("2. View the first 10 tunes") #allows viewing of first 10 tunes
    print("3. Search by title") 
    print("4. Filter by book number")
    print("5. Filter by tune type")
    print("6. Filter by key")
    print("0. Exit")
    print("="*50)


def load_files_option():
    """
    Option 1: Load ABC files into database
    
    This function:
    1. Finds all .abc files
    2. Parses them to extract tune data
    3. Inserts the data into the database
    """
    print("\nLoading ABC files...")
    
    # Step 1: Find all ABC files in abc_books folder
    abc_files = find_abc_files()
    if not abc_files:
        print("No files found!")
        return
    
    print(f"\nFound {len(abc_files)} files")
    
    # Step 2: Parse each file to get tune data
    all_tunes = []  # List to collect all tunes from all files
    for filepath, book_number in abc_files:
        print(f"\nParsing: {filepath}")
        tunes = parse_abc_file(filepath, book_number)
        all_tunes.extend(tunes)  # Add these tunes to our master list
    
    print(f"\nTotal tunes parsed: {len(all_tunes)}")
    
    # Step 3: Insert into database
    if all_tunes:
        # Ask user if they want to clear old data first
        choice = input("\nClear existing data first? (y/n): ")
        if choice.lower() == 'y':
            conn = connectToDB()
            cursor = conn.cursor()
            # SQL DELETE statement removes all rows from table
            cursor.execute("DELETE FROM music")
            conn.commit()
            conn.close()
            print("Cleared existing data")
        
        # Insert all tunes
        insert_tunes_to_db(all_tunes)


def view_10_option(df):
    """
    Option 2: View the first 10 tunes
    
    SQL used: SELECT * FROM music (in load_data_to_dataframe function)
    This returns ALL tunes from the database
    
    Args:
        df: existing DataFrame (or None if not loaded yet)
    
    Returns: DataFrame with all data
    """
    # If we haven't loaded data yet, load it now
    if df is None or df.empty:
        df = load_data_to_dataframe()
    
    # Show basic info
    print(f"\nTotal tunes: {len(df)}")
    print("\nFirst 10 tunes:")
    
    # Show only certain columns (title, book_number, etc.) and only first 10 rows
    print(df[['title', 'book_number', 'tune_type', 'key_signature']].head(10))
    
    return df


def search_by_title(df):
    """
    Option 3: Search by title
    
    Uses pandas filtering: df[condition]
    The condition checks if title contains the search term
    This is like SQL: SELECT * FROM music WHERE title LIKE '%search%'
    
    Args:
        df: DataFrame with tune data
    
    Returns: DataFrame (unchanged)
    """
    if df is None or df.empty:
        print("Please load data first (option 2)")
        return df
    
    # Get search term from user
    search = input("\nEnter title to search: ")
    
    # Filter DataFrame where title contains the search term
    # str.contains() checks if the search term appears anywhere in the title
    # case=False makes it case-insensitive (finds "Jig" and "jig")
    # na=False means treat blank/null values as not matching
    results = df[df['title'].str.contains(search, case=False, na=False)]
    
    # Display results
    print(f"\nFound {len(results)} results:")
    if not results.empty:
        print(results[['title', 'book_number', 'tune_type', 'key_signature']])
    
    return df


def filter_by_book(df):
    """
    Option 4: Filter by book number
    
    Uses pandas filtering: df[condition]
    This is like SQL: SELECT * FROM music WHERE book_number = 1
    
    Args:
        df: DataFrame with tune data
    
    Returns: DataFrame (unchanged)
    """
    if df is None or df.empty:
        print("Please load data first (option 2)")
        return df
    
    # Get book number from user
    book = input("\nEnter book number: ")
    
    # Filter DataFrame where book_number equals what user entered
    # .astype(str) converts book_number to string so we can compare with user input
    results = df[df['book_number'].astype(str) == book]
    
    # Display results
    print(f"\nFound {len(results)} tunes in book {book}:")
    if not results.empty:
        print(results[['title', 'tune_type', 'key_signature']].head(20))
    
    return df


def filter_by_type(df):
    """
    Option 5: Filter by tune type (rhythm)
    
    Uses pandas filtering with str.contains()
    This is like SQL: SELECT * FROM music WHERE tune_type LIKE '%jig%'
    
    Args:
        df: DataFrame with tune data
    
    Returns: DataFrame (unchanged)
    """
    if df is None or df.empty:
        print("Please load data first (option 2)")
        return df
    
    # Get tune type from user
    tune_type = input("\nEnter tune type (e.g., jig, reel): ")
    
    # Filter DataFrame where tune_type contains the search term
    results = df[df['tune_type'].str.contains(tune_type, case=False, na=False)]
    
    # Display results
    print(f"\nFound {len(results)} tunes:")
    if not results.empty:
        print(results[['title', 'book_number', 'key_signature']].head(20))
    
    return df


def filter_by_key(df):
    """
    Option 6: Filter by key signature
    
    Uses pandas filtering with str.contains()
    This is like SQL: SELECT * FROM music WHERE key_signature LIKE '%G%'
    
    Args:
        df: DataFrame with tune data
    
    Returns: DataFrame (unchanged)
    """
    if df is None or df.empty:
        print("Please load data first (option 2)")
        return df
    
    # Get key from user
    key = input("\nEnter key (e.g., G, D, A): ")
    
    # Filter DataFrame where key_signature contains the search term
    results = df[df['key_signature'].str.contains(key, case=False, na=False)]
    
    # Display results
    print(f"\nFound {len(results)} tunes:")
    if not results.empty:
        print(results[['title', 'book_number', 'tune_type']].head(20))
    
    return df


def main():
    """
    Main program - this is what runs when the script starts
    
    UI STRUCTURE:
    1. Display menu (list of options)
    2. Get user input (which option they choose)
    3. Run the corresponding function
    4. Repeat until user chooses to exit
    
    This is a simple menu-driven UI using a while loop
    """
    print("="*50)
    print("ABC MUSIC DATABASE APPLICATION")
    print("="*50)
    
    # Test database connection at startup
    try:
        conn = connectToDB()
        print("Connected to database successfully")
        conn.close()
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
        print("Please check your database settings")
        return
    
    # df will store our DataFrame (starts as None until we load data)
    df = None
    
    # Main menu loop - keeps running until user exits
    while True:
        # Show the menu
        display_menu()
        
        # Get user's choice
        choice = input("\nEnter choice: ")
        
        # Run the appropriate function based on user's choice
        if choice == '0':
            print("\nGoodbye!")
            break  # Exit the loop (end program)
        elif choice == '1':
            load_files_option()
        elif choice == '2':
            df = view_10_option(df)
        elif choice == '3':
            df = search_by_title(df)
        elif choice == '4':
            df = filter_by_book(df)
        elif choice == '5':
            df = filter_by_type(df)
        elif choice == '6':
            df = filter_by_key(df)
        else:
            print("Invalid choice")


# This final piece runs when you execute the script
if __name__ == "__main__":
    main()