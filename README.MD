# Data Centric Programming Assignment 2025

- [Assignment Brief](assignment.md)

Name: Laurentiu Codreanu

Student Number: C24387041

# Screenshots

# Description of the project
For this assignment, I created a Pyhton program that works with the collection of ABC music notation files and stores the information in a MySQL database. The program searches through folders of abc. files, reads and parses each tune, and extracts details like the ID, Book Number, Tune ID, Title, Alt Title, Tune Type, Key Signature and Notation. 

After parsing the files, the program inserts all the data into a database table.

Once the data is stored, the program also includes a simple text-based menu that lets the user interact with the database. Using the interactive menu users can load all the ABC files, view the first 10 tunes, search a tune by title, filter tunes by book number, filter tunes by rhythm type or key.

I also used pandas to load the data into a DataFrame for easier filtering and display.

Overall, this program includes file handling, text parsing, database operatiions, and basic data analysis in one application.

# Instructions for use

1. Set Up the Database
-Install and run MySQL on your machine
-Create a database called "abc_books"
-Inside it, create a table named music with these columns: id, book_number, tune_id, title, alt_title, tune_type, key_signature, notation.
-If your MySQL username or password is different from the script, update it in the connectToDb() function

2. Prepare the ABC files
-In the main project folder, make sure there is a directory called abc_books
Each subfolder inside should be named with its book number ( 1 & 2 )

3. Run the Program
-Open the script in VSCode and run it
-Make sure the required Python modules are installed like mysql.connector, pandas, os etc..

4. Using the Menu
When the program starts, a text-based menu will appear.
-The options let you:
Option 1 = Load and parse all ABC files, and insert them into the Database.
Option 2 = View the first 10 tunes
Option 3 = Search tunes by title
Option 4 = Filter tunes by book number
Option 5 = Filter tunes by tune type
Option 6 = Filter tunes by key signature 
Option 0 = Exit the Program

5. Loading Data
-When selecting option 1, you'll be asked if you want to delete existing data from the table first.
-Type 'y' to clear everything or 'n' to keep old inputs

6. Viewing Results
-The results from from options 2-6 are shown in the concsole using pandas DataFrames
-Only the most important columns are printed, so the output is easy to read.

7. Exit
-Enter '0' at any time to quit the application.

# How it works:

# List of files in the project

| Files | Source |
|-----------|-----------|
| main.py | Self written |
| test.py | Modified from [reference]() |
| test.py | From [reference]() |

# References
* Item 1
* Item 2

# What I am most proud of in the assignment

Bla bla bla

# What I learned

Bla bla bla

## Below is how to use Markdown. You can delete this:

## This is how to markdown text:

This is *emphasis*

This is a bulleted list

- Item
- Item

This is a numbered list

1. Item
1. Item

This is a [hyperlink](http://bryanduggan.org)

# Headings
## Headings
#### Headings
##### Headings

This is code:

```Java
public void render()
{
	ui.noFill();
	ui.stroke(255);
	ui.rect(x, y, width, height);
	ui.textAlign(PApplet.CENTER, PApplet.CENTER);
	ui.text(text, x + width * 0.5f, y + height * 0.5f);
}
```

So is this without specifying the language:

```
public void render()
{
	ui.noFill();
	ui.stroke(255);
	ui.rect(x, y, width, height);
	ui.textAlign(PApplet.CENTER, PApplet.CENTER);
	ui.text(text, x + width * 0.5f, y + height * 0.5f);
}
```

This is an image using a relative URL:

![An image](images/p8.png)

This is an image using an absolute URL:

![A different image](https://bryanduggandotorg.files.wordpress.com/2019/02/infinite-forms-00045.png?w=595&h=&zoom=2)

This is a youtube video:

[![YouTube](http://img.youtube.com/vi/J2kHSSFA4NU/0.jpg)](https://www.youtube.com/watch?v=J2kHSSFA4NU)

This is a table:

| Heading 1 | Heading 2 |
|-----------|-----------|
|Some stuff | Some more stuff in this column |
|Some stuff | Some more stuff in this column |
|Some stuff | Some more stuff in this column |

|Some stuff | Some more stuff in this column |





