from py2neo import Graph
import pandas as pd
from src import queries

#load functions to update neo4j db (Nodes/Relations/Propertys/Indexes/Constraints)

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

#set numbers on neo4j db as floats, normally it uploads as string
def stringsToFloats():
    query = "MATCH (cp:ClientProfile),(cc:CreditCard),()-[r:FIDELITY]->(),()-[r2:USES_CARD]->() "\
            "SET cp.age = toFloat(cp.age), cp.dependents = toFloat(cp.dependents), cc.available_limit = toFloat(cc.available_limit), cc.limit = toFloat(cc.limit), cc.number_txs12 = toFloat(cc.number_txs12), cc.number_txs_variation_Q4_Q1 = toFloat(cc.number_txs_variation_Q4_Q1), cc.txs12 = toFloat(cc.txs12), cc.txs_variation_Q4_Q1 = toFloat(cc.txs_variation_Q4_Q1), cc.used_limit = toFloat(cc.used_limit), r.inactivity12 = toFloat(r.inactivity12), r.months_as_client = toFloat(r.months_as_client), r.products_subscribed = toFloat(r.products_subscribed), r.support_requests12 = toFloat(r.support_requests12), r2.utilization_rate = toFloat(r2.utilization_rate) "
    graph.run(query)

#calculates a score from Fidelity relation data between client category and profile, adds it to profile node
def addFidelityScore():
    query = "MATCH ()-[r:FIDELITY]->() "\
            "WITH  "\
            "MERGE (cp)-[r:HAS_EDUCATION]->(e) "
    graph.run(query)

graph = Graph("http://neo4j:password@localhost:7474")