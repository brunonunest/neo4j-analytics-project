from py2neo import Graph

#load functions to update neo4j db (Nodes/Relations/Propertys/Indexes/Constraints)
#1
def insertClientCategory():
    query = "LOAD CSV WITH HEADERS FROM 'file:///clientsheet.csv' AS line "\
            "MERGE (c:ClientCategory {label: line.Category}) "
    graph.run(query)
#2
def insertFidelityRelClientProfile():
    query = "LOAD CSV WITH HEADERS FROM 'file:///clientsheet.csv' AS line "\
            "MATCH (c:ClientCategory {label: line.Category}) "\
            "MERGE (cp:ClientProfile {clientnum: line.CLIENTNUM}) "\
            "WITH toFloat(replace(line.Age, ',', '.')) as age, toFloat(replace(line.Dependents, ',', '.')) as dependents, cp, line, c "\
            "SET cp.age = age, cp.gender = line.Gender, cp.dependents = dependents, cp.marital_status = line.MaritalStatus "\
            "MERGE (cp)-[r:FIDELITY]->(c) "\
            "WITH toFloat(replace(line.ProductsSubscribed, ',', '.')) as products_subscribed, toFloat(replace(line.Inactivity12months, ',', '.')) as inactivity12, toFloat(replace(line.CustomerSupportRequests12months, ',', '.')) as support_requests12, toFloat(replace(line.MonthsasClient, ',', '.')) as months_as_client, r "\
            "SET r.products_subscribed = products_subscribed, r.inactivity12 = inactivity12, r.support_requests12 = support_requests12, r.months_as_client = months_as_client "
    graph.run(query)
#3
def insertIncome():
    query = "LOAD CSV WITH HEADERS FROM 'file:///clientsheet.csv' AS line "\
            "MATCH (cp:ClientProfile {clientnum: line.CLIENTNUM}) "\
            "MERGE (s:Salary {anual: line.AnualIncome}) "\
            "MERGE (cp)-[r:HAS_INCOME]->(s) "
    graph.run(query)
#4
def insertEducation():
    query = "LOAD CSV WITH HEADERS FROM 'file:///clientsheet.csv' AS line "\
            "MATCH (cp:ClientProfile {clientnum: line.CLIENTNUM}) "\
            "MERGE (e:Education {label: line.Education}) "\
            "MERGE (cp)-[r:HAS_EDUCATION]->(e) "
    graph.run(query)
#5
def insertCreditCard():
    query = "LOAD CSV WITH HEADERS FROM 'file:///clientsheet.csv' AS line "\
            "MATCH (cp:ClientProfile {clientnum: line.CLIENTNUM}) "\
            "WITH toFloat(replace(line.CardUtilizationRate, ',', '.')) as utilization_rate, toFloat(replace(line.Limit, ',', '.')) as limit, toFloat(replace(line.UsedLimit, ',', '.')) as used_limit, toFloat(replace(line.AvailableLimit, ',', '.')) as available_limit, toFloat(replace(line.TransactionsvaluevariationQ4_Q1, ',', '.')) as txs_variation_Q4_Q1, toFloat(replace(line.Transactionsvalue12months, ',', '.')) as txs12, toFloat(replace(line.Transactions12months, ',', '.')) as number_txs12, toFloat(replace(line.VariationTransactionsQ4_Q1, ',', '.')) as number_txs_variation_Q4_Q1, line, cp "\
            "MERGE (cp)-[r:USES_CARD {utilization_rate: utilization_rate}]->(cc:CreditCard {type: line.CardType, limit: limit, used_limit: used_limit, available_limit: available_limit, txs_variation_Q4_Q1: txs_variation_Q4_Q1, txs12: txs12, number_txs12: number_txs12, number_txs_variation_Q4_Q1: number_txs_variation_Q4_Q1}) "
    graph.run(query)

#This should be run only once, after creating the model, creates indexes to query faster and unique constraints when needed
def createIndexes():
    indexsalary = "CREATE CONSTRAINT unique_salary_anual IF NOT EXISTS FOR (d:Salary) REQUIRE d.anual IS UNIQUE"
    graph.run(indexsalary)
    indexeducation = "CREATE CONSTRAINT unique_education_label IF NOT EXISTS FOR (d:Education) REQUIRE d.label IS UNIQUE"
    graph.run(indexeducation)
    indexclient = "CREATE CONSTRAINT unique_clientnum IF NOT EXISTS FOR (d:ClientProfile) REQUIRE d.clientnum IS UNIQUE"
    graph.run(indexclient)
    indexcategory = "CREATE CONSTRAINT unique_client_category IF NOT EXISTS FOR (d:ClientCategory) REQUIRE d.label IS UNIQUE"
    graph.run(indexcategory)
    indexcards = "CREATE INDEX index_for_cards IF NOT EXISTS FOR (d:CreditCard) ON d.type"
    graph.run(indexcards)
    
graph = Graph("http://neo4j:password@localhost:7474")