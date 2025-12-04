# Data Centric Programming Assignment 2025

- [Assignment Brief](assignment.md)

Name: Laurentiu Codreanu

Student Number: C24387041

# Screenshots
Screenshot of Interactive Menu
![Alt text](https://github.com/LaurCD/dcp25-assignment/blob/main/images/Screenshot%202025-12-04%20at%2015.46.12.png)

Screenshot of Tune Parsing
![Alt Text](https://github.com/LaurCD/dcp25-assignment/blob/main/images/Screenshot%202025-12-04%20at%2015.46.44.png)

Screenshot of first 10 Tunes being Displayed
![Alt Text](https://github.com/LaurCD/dcp25-assignment/blob/main/images/Screenshot%202025-12-04%20at%2015.47.22.png)

Screenshot showing Menu return results when searching tunes by Title
![Alt Text](https://github.com/LaurCD/dcp25-assignment/blob/main/images/Screenshot%202025-12-04%20at%2015.48.42.png)

# Description of the project
For this assignment, I created a Pyhton program that works with the collection of ABC music notation files and stores the information in a MySQL database. The program searches through folders of abc. files, reads and parses each tune, and extracts details like the ID, Book Number, Tune ID, Title, Alt Title, Tune Type, Key Signature and Notation. 

After parsing the files, the program inserts all the data into a database table.

Once the data is stored, the program also includes a simple text-based menu that lets the user interact with the database. Using the interactive menu users can load all the ABC files, view the first 10 tunes, search a tune by title, filter tunes by book number, filter tunes by rhythm type or key.

I also used pandas to load the data into a DataFrame for easier filtering and display.

Overall, this program includes file handling, text parsing, database operatiions, and basic data analysis in one application.

# Instructions for use

1. Set Up the Database:
Install and run MySQL on your machine
Create a database called "abc_books"
Inside it, create a table named music with these columns: id, book_number, tune_id, title, alt_title, tune_type, key_signature, notation.
If your MySQL username or password is different from the script, update it in the connectToDb() function

2. Prepare the ABC files:
In the main project folder, make sure there is a directory called abc_books
Each subfolder inside should be named with its book number ( 1 & 2 )

3. Run the Program:
Open the script in VSCode and run it
Make sure the required Python modules are installed like mysql.connector, pandas, os etc..

4. Using the Menu:
When the program starts, a text-based menu will appear.
The options let you:
Option 1 = Load and parse all ABC files, and insert them into the Database.
Option 2 = View the first 10 tunes
Option 3 = Search tunes by title
Option 4 = Filter tunes by book number
Option 5 = Filter tunes by tune type
Option 6 = Filter tunes by key signature 
Option 0 = Exit the Program

5. Loading Data:
When selecting option 1, you'll be asked if you want to delete existing data from the table first.
Type 'y' to clear everything or 'n' to keep old inputs

6. Viewing Results:
The results from from options 2-6 are shown in the concsole using pandas DataFrames
Only the most important columns are printed, so the output is easy to read.

7. Exit:
Enter '0' at any time to quit the application.

# How it works:
This program is designed to take a collection of ABC music notation files, extract useful information from them, and store everything neatly in a MySQL database. Heres how everything worked:

Finding the Files:
The script looks through the abc_books directory.
Each folder inside ( named numbers 1 & 2 ) represents a book.
All .abc files inside these folders are collected and sent for parsing.

Parsing Each ABC File:
The program reads each file line by line.
When it finds an X: tag, it starts a new tune
It extracts:
X: tune reference number
T: Title of tune
R: rhythm ( tune type )
K: key signature 
any remaining lines goes into music notation
Each tune is stored in a dictionary.

Inserting into the Database:
After parsing, each tunes dictionary is mapped to the appropriate database columns.
The program uses an SQL INSERT statement to add the tune into the music table.
Users can choose to clear old data before inserting new tunes.

Loading Data with Pandas:
The script can load the entire music table into pandas DataFrame.
This makes searching, filtering, and displaying results simple and readable.

Interactive Menu:
The main loop presents the user with options.
Each menu choice calls a specific function like:
Loading files
Viewing the first 10 tunes
Searching a tune by title
And filtering by book, tune type, or key
Results are printed directly in the console

Program Flow:
The program keeps running until the user selects option '0' to exit.
It is designed to be simple to use while showing how file parsing, databases, and pandas can work together.


# List of files in the project

| Files     | Source    |
|-----------|-----------|
| assing1.py | Starter code + self written + modified from [reference]() |
| abc_books  | loaded from git fork |

# References
| Topics    |  Source   |
|-----------|-----------|
| File Loading & Parsing  | [Website](https://realpython.com/read-write-files-python/) |
| File Loading & Parsing  |  [YouTube](https://www.youtube.com/watch?v=6Ar7sf6aU_M&list=PLMSYnjOjlErdyy83UXCykRV0-dXAMtFW-) |
| test.py | From [reference]() |
| test.py | From [reference]() |
| test.py | From [reference]() |
| test.py | From [reference]() |
| test.py | From [reference]() |
| test.py | From [reference]() |
| test.py | From [reference]() |


# What I am most proud of in the assignment

Bla bla bla

# What I learned

Bla bla bla



