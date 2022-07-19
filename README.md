Data Analysis project example using neo4j + python

# How to run

1. After installing python, virtualenv and neo4j, clone the repo locally
2. Start neo4j Desktop and create a database locally
3. Only python lib used is py2neo, install using `pip install py2neo`
4. Run the functions at `functions.py` one by one, the number order is commented, including the .csv file on your neo4j db instance `import` folder 
5. The last function `createIndexes` needs to be run only once, to create the indexes + constraints
6. After that check neo4j browser and bloom to see your database uploaded, you are ok to start your analysis and development

**`insightqueries.txt` contains the queries used to get insights

** The problem and method to solve it presented at `presentation.pptx`
