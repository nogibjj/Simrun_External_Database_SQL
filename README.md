## SQLite Lab [![CI](https://github.com/nogibjj/Simrun_sqlite-lab/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/Simrun_sqlite-lab/actions/workflows/cicd.yml)
![Alt text](<Diagram.png>)'

### Description of my dataset I choose:
This dataset provides information from different episodes from the TV show Jeopardy! It includes details like the show number, air date, round of the game, category of the question, value of the question, the question itself, and the correct answer. Each row represents a different question asked in the episode.

### Creating the three different .py files in mylib required for the SQLite Lab

### Extract.py

Within this file is the extract function that is called in the main.py. The function takes in a URL which the request library fetches the dataset and then saves a copy of this data as a csv in binary format in my repository.

### Query.py
Within this file is the query function that is called in the main.py. The function takes in a query and uses sqlite3 to communicate this query with the database. We establish a connection with sqlite3.connect with the database. Then we create a pointer with conn.cursor() and this tells sqlite3 where to look in the database based on the query. cursor.execute tells sqlite3 to read the query and get all the information wanted from the database. Con.close() puts all the information back in the database once we have looked at it. 

### Transfor_load.py
Within this file is the load function that is called in the main.py. This takes in the dataset and creates a database that can be found in the repository. It then takes the dataset which is converted into a dataframe and makes it into a sql table. This SQL table can then be queried to retrieve specific information from the dataset as needed.

#### Reflection Questions

* What challenges did you face when extracting, transforming, and loading the data? How did you overcome them?
  - I struggled with understanding the difference between extracting the data versues loading the data when I was creating the functions. 
* What insights or new knowledge did you gain from querying the SQLite database?
    - I learned that SQLite is a translator that goes to the database with our query to get the information we want from the database. 
* How can SQLite and SQL help make data analysis more efficient? What are the limitations?
    - SQL can let us join multiple tables based on related columns which allows for better integration of diverse data sources. However, SQLite can be slowere than larger database systmes for complex queries especially when there are multiple users accesssing the database.
* What AI assistant did you use and how did it compare to others you've tried? What are its strengths and weaknesses?
    - I used Github Copilot. This is really great as an AI tool as it offers help with code syntax and code options. Disadvantage being that sometimes it offers really long code that is unnecessarily verbose and might impact readability. 
* If you could enhance this lab, what would you add or change? What other data would be interesting to load and query?
  - I would like to try and integrate different databases and try making queries from each of the databases to see if I able to do so. 
