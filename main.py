import csv
import json
import uuid
import time
import numpy as np

from pymongo import MongoClient
from pymongo.cursor import Cursor
from time import process_time




def insert_test_gene_data():
    client = MongoClient('128.163.202.61', 27017, username='admin', password='cody01')
    db = client['digital_genomic']
    gene_collection = db['test_genes']

    raw_gene_data = '/Users/dylan/Downloads/deident_heme_reports.json/deident_heme_reports.json'

    with open(raw_gene_data) as f:
        gene_data = json.load(f)


    for gene in gene_data:
        print(gene['id'])
        gene_collection.insert_one(gene)

    client.close()

def AVG_Freq():
    client = MongoClient('128.163.202.61', 27017, username='admin', password='cody01')
    db = client['digital_genomic']
    gene_collection = db['test_genes']

    agr = [
        {"$project": {'mutations': 1}},
        {"$unwind": "$mutations"},
        {"$project": {'mutations.allelefreq': 1}},
        {'$group': {'_id': None, 'total_avg_freq': {'$avg': '$mutations.allelefreq'}}},
        {"$project": {'_id': 0, 'total_avg_freq': 1}}
    ]

    agg_result = gene_collection.aggregate(agr)

    for i in agg_result:
        print(i)

    client.close()

def get_requested_values(value):
    client = MongoClient('128.163.202.61', 27017, username='admin', password='cody01')
    db = client['digital_genomic']
    gene_collection = db['test_genes']

    val_to_get = "mutations.{}".format(value)

    agr = [
        {"$project": {'_id' : 0, 'id': 1, 'mutations': 1}},
        {"$unwind": "$mutations"},
        {"$project": {'id': 1, val_to_get: 1}},
    ]

    agg_result = gene_collection.aggregate(agr)

    for i in agg_result:
        print(i)

    client.close()

def num_genes():
    client = MongoClient('128.163.202.61', 27017, username='admin', password='cody01')
    db = client['digital_genomic']
    gene_collection = db['test_genes']

    agr = [
        {"$project": {'_id' : 0, 'id': 1, 'mutations': 1}},
    ]

    agg_result = gene_collection.aggregate(agr)

    counter = 0

    for i in agg_result:
        counter = counter + 1


    return counter

def percentage_get(field, field_value):
    client = MongoClient('128.163.202.61', 27017, username='admin', password='cody01')
    db = client['digital_genomic']
    gene_collection = db['test_genes']

    val_to_get = "mutations.{}".format(field)

    agr = [
        {"$project": {'_id' : 0, 'id': 1, 'mutations': 1}},
        {"$unwind": "$mutations"},
        {"$project": {'id': 1, val_to_get: 1}},
        {"$match": {val_to_get: field_value}},
    ]

    agg_result = gene_collection.aggregate(agr)

    tot_num = num_genes()
    num_field = 0

    for i in agg_result:
        print(i)
        num_field = num_field + 1

    client.close()

    return (num_field/tot_num) * 100


#AVG_Freq()
#get_requested_values("gene")
the_percentage = percentage_get("gene", "TP53")
print(the_percentage)
