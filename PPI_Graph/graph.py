import os
from dotenv import load_dotenv
import pickle

from neo4j import GraphDatabase

load_dotenv()
threshold = 4
with open("../distance_dict.pkl", "rb") as f:
    distance_dict = pickle.load(f)
print("Distances loaded from pickle.")

edges = []
for key, value in distance_dict.items():
    if value <= threshold:
        alpha1, alpha2 = key.split("<--->")
        edges.append((alpha1, alpha2,value))
print("Edges identified.")

url = os.getenv("URL")
username = os.getenv("NEO4j_USERNAME")
password = os.getenv("PASSWORD")

# create a driver
driver = GraphDatabase.driver(url, auth=(username, password))
print('Driver created')


def create_edge(tx, alpha1, alpha2,distance):
    # alpha1 and alpha 2 will be like-> AA27-6wps
    # name = symbol + chain + seq + pdbid
    # sy1 = alpha1[0]
    ch1 = alpha1[1]
    # sq1 = alpha1[2:4]
    # pdbid1 = alpha1[5:]

    # sy2 = alpha2[0]
    ch2 = alpha2[1]
    # sq2 = alpha2[2:4]
    # pdbid2 = alpha2[5:]

    tx.run("MERGE (a1:AMINO_ACID {name:$alpha1, chain:$ch1}) "
           "MERGE (a2:AMINO_ACID {name:$alpha2, chain:$ch2}) "
           "MERGE (a1)-[:CONNECTED_TO {distance:$distance}]->(a2)", alpha1=alpha1, alpha2=alpha2, ch1=ch1, ch2=ch2,distance=distance)


with driver.session() as session:
    # session.write_transaction(create_edge, "AA27-6wps", "YA28-6wps")
    # session.write_transaction(create_edge, "YA28-6wps", "AA27-6wps")
    for i, edge in enumerate(edges):
        alpha1 = edge[0]
        alpha2 = edge[1]
        distance = edge[2]
        session.write_transaction(create_edge, alpha1, alpha2,distance)
        session.write_transaction(create_edge, alpha2, alpha1,distance)
        if (i + 1) % 100 == 0:
            print(i + 1, "records added.")

# with driver.session() as session:
#     session.write_transaction(create_edge, "AA27-6wps", "WA64-6wps")
#     session.write_transaction(create_edge, "WA64-6wps", "AA27-6wps")

driver.close()
print("Connection closed, Done !")
