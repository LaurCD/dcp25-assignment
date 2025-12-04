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



