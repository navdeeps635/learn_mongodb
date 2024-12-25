import os
import pprint
from bson.objectid import ObjectId
from pymongo.mongo_client import MongoClient
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())


password = os.getenv("MONDODB_PWD")

connection_string = f"mongodb+srv://badhan45457:{password}@tutorial.hsmox.mongodb.net/?retryWrites=true&w=majority&appName=tutorial"
print(connection_string)

# Create a new client and connect to the server
client = MongoClient(connection_string)

dbs = client.list_database_names()
print(dbs)
test_db = client.test
test_collection = test_db.test

def insert_test_doc(collection):
    test_doc = {
        "name": "Tim",
        "type": "Test"
    }
    inserted_id = collection.insert_one(test_doc).inserted_id
    print(inserted_id)

# insert_test_doc(collection=test_collection)

production = client.production
person_collection = production.person_collection

def create_documents(collection):
    first_names = ["Tim", "Sarah", "Jennifer", "Jose", "Brad", "Allen"]
    last_names = ["Ruscica", "Smith", "Bart", "Cater", "Pit", "Geral"]
    ages = [21, 40, 23, 19, 34, 67]

    docs = []
    for first_name, last_name, age in zip(first_names, last_names, ages):
        doc = {"first_name": first_name, "last_name": last_name, "age": age}
        docs.append(doc)
    collection.insert_many(docs)


# create_documents(collection=person_collection)
# print(production.list_collection_names())
printer = pprint.PrettyPrinter()

def find_all_people():
    people = person_collection.find()

    for person in people:
        printer.pprint(person)

# find_all_people()

def find_tim():
    tim = person_collection.find_one({"first_name": "Tim", "last_name": "Ruscica"})
    printer.pprint(tim)

# find_tim()    

def count_all_people():
    count = person_collection.count_documents(filter={})
    
    print("No. of people:", count)

# count_all_people()

def get_person_by_id(person_id):
    _id = ObjectId(person_id)
    person = person_collection.find_one({"_id": _id})
    printer.pprint(person)

# get_person_by_id(person_id="676bff92063b410139606134")

def get_age_range(min_age, max_age):
    query = {
        "$and": [
            {"age": {"$gte": min_age}},
            {"age": {"$lte": max_age}}
        ]
    }
    people = person_collection.find(query).sort("age")
    for person in people:
        printer.pprint(person)

# get_age_range(20, 35)

def project_columns():
    columns = {
        "_id": 0, "first_name": 1, "last_name": 1 # 0 means i don't want those columns in output and for 1, vice versa.
    }
    people = person_collection.find({}, columns)
    for person in people:
        printer.pprint(person)

# project_columns()

def update_person_by_id(person_id):
    _id = ObjectId(person_id)

    # all_updates = {
    #     "$set": {"new_field": True},
    #     "$inc": {"age": 1},
    #     "$rename": {"first_name": "first", "last_name": "last"}
    # }

    # person_collection.update_one({"_id": _id}, all_updates)

    person_collection.update_one({"_id": _id}, {"$unset": {"new_field": ""}})

# update_person_by_id("676bff92063b410139606136")

def replace__one(person_id):
    _id = ObjectId(person_id)

    new_doc = {
        "first_name": "new first name",
        "last_name": "new last name",
        "age": 100
    }
    person_collection.replace_one({"_id": _id}, new_doc)

# replace__one("676bff92063b410139606136")

def delete_docs_by_id(person_id):
    _id = ObjectId(person_id)
    person_collection.delete_one({"_id": _id})

delete_docs_by_id("676bff92063b410139606136")