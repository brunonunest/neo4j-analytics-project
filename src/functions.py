from py2neo import Graph
import pandas as pd
from src import queries

#load functions to upload data to neo4j db (Nodes/Relations/Propertys)

def insertClientCategory():
    query = "LOAD CSV WITH HEADERS FROM 'file:///clientsheet.csv' AS line "\
            "MERGE (c:ClientCategory {label: line.Category}) "
    graph.run(query)

def insertFidelityRelClientProfile():
    query = "LOAD CSV WITH HEADERS FROM 'file:///clientsheet.csv' AS line "\
            "MATCH (c:ClientCategory {label: line.Category}) "\
            "MERGE (cp:ClientProfile {clientnum: line.CLIENTNUM}) "\
            "SET cp.age = line.Age, cp.gender = line.Gender, cp.dependents = line.Dependents, cp.marital_status = line.MaritalStatus "\
            "MERGE (cp)-[r:FIDELITY]->(c) "\
            "SET r.products_subscribed = line.ProductsSubscribed, r.inactivity12 = line.Inactivity12months, r.support_requests12 = line.CustomerSupportRequests12months, r.months_as_client = line.MonthsasClient "
    graph.run(query)

def insertIncome():
    query = "LOAD CSV WITH HEADERS FROM 'file:///clientsheet.csv' AS line "\
            "MATCH (cp:ClientProfile {clientnum: line.CLIENTNUM}) "\
            "MERGE (s:Salary {anual: line.AnualIncome}) "\
            "MERGE (cp)-[r:HAS_INCOME]->(s) "
    graph.run(query)

def insertEducation():
    query = "LOAD CSV WITH HEADERS FROM 'file:///clientsheet.csv' AS line "\
            "MATCH (cp:ClientProfile {clientnum: line.CLIENTNUM}) "\
            "MERGE (e:Education {label: line.Education}) "\
            "MERGE (cp)-[r:HAS_EDUCATION]->(e) "
    graph.run(query)

def insertCreditCard():
    query = "LOAD CSV WITH HEADERS FROM 'file:///clientsheet.csv' AS line "\
            "MATCH (cp:ClientProfile {clientnum: line.CLIENTNUM}) "\
            "MERGE (cp)-[r:USES_CARD {utilization_rate: line.CardUtilizationRate}]->(cc:CreditCard {type: line.CardType, limit: line.Limit, used_limit: line.UsedLimit, available_limit: line.AvailableLimit, txs_variation_Q4_Q1: line.TransactionsvaluevariationQ4_Q1, txs12: line.Transactionsvalue12months, number_txs12: line.Transactions12months, number_txs_variation_Q4_Q1: line.VariationTransactionsQ4_Q1}) "
    graph.run(query)

#calculates a score from Fidelity relation data between client category and profile, adds it to profile node
def addFidelityScore():
    pass

graph = Graph("http://neo4j:password@localhost:7474")