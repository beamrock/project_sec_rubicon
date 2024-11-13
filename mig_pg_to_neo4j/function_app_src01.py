import azure.functions as func
import logging
import os
from neo4j import GraphDatabase
import psycopg2
import pandas as pd
from decimal import Decimal

# Neo4j and PostgreSQL connection details from environment variables
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_DATABASE = "secdb"
PG_HOST = os.getenv("PG_HOST")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# Neo4j driver
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
app = func.FunctionApp()

def delete_all_nodes_in_neo4j():
    try:
        with driver.session(database=NEO4J_DATABASE) as session:
            session.write_transaction(lambda tx: tx.run("MATCH (n) DETACH DELETE n"))
        logging.info("Successfully deleted all nodes in Neo4j.")
    except Exception as e:
        logging.error(f"Error deleting all nodes in Neo4j: {e}")
        raise

def fetch_data_from_postgres():
    try:
        # PostgreSQL database connection
        connection = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        
        # Load data from PostgreSQL into DataFrames
        person_df = pd.read_sql("SELECT name, born FROM person", connection)
        movie_df = pd.read_sql("SELECT title, released, tagline FROM movie", connection)
        acted_in_df = pd.read_sql("SELECT name, title, roles FROM acted_in", connection)

        # Close the connection
        connection.close()
        
        logging.info("Successfully fetched data from PostgreSQL.")
        return person_df, movie_df, acted_in_df
    except Exception as e:
        logging.error(f"Error fetching data from PostgreSQL: {e}")
        raise

def create_person_nodes_in_neo4j(person_df):
    try:
        with driver.session(database=NEO4J_DATABASE) as session:
            for _, row in person_df.iterrows():
                name, born = row['name'], row['born']
                # Convert born to int if it’s a Decimal
                if isinstance(born, Decimal):
                    born = int(born)
                session.write_transaction(
                    lambda tx: tx.run(
                        "CREATE (p:Person {name: $name, born: $born})",
                        name=name, born=born
                    )
                )
        logging.info("Successfully created person nodes in Neo4j.")
    except Exception as e:
        logging.error(f"Error creating person nodes in Neo4j: {e}")
        raise

def create_movie_nodes_in_neo4j(movie_df):
    try:
        with driver.session(database=NEO4J_DATABASE) as session:
            for _, row in movie_df.iterrows():
                title, released, tagline = row['title'], row['released'], row['tagline']
                # Convert released to int if it’s a Decimal
                if isinstance(released, Decimal):
                    released = int(released)
                session.write_transaction(
                    lambda tx: tx.run(
                        "CREATE (m:Movie {title: $title, released: $released, tagline: $tagline})",
                        title=title, released=released, tagline=tagline
                    )
                )
        logging.info("Successfully created movie nodes in Neo4j.")
    except Exception as e:
        logging.error(f"Error creating movie nodes in Neo4j: {e}")
        raise

def create_acted_in_relationships_in_neo4j(acted_in_df):
    try:
        with driver.session(database=NEO4J_DATABASE) as session:
            for _, row in acted_in_df.iterrows():
                name, title, roles = row['name'], row['title'], row['roles']
                session.write_transaction(
                    lambda tx: tx.run(
                        """
                        MATCH (p:Person {name: $name})
                        MATCH (m:Movie {title: $title})
                        CREATE (p)-[:ACTED_IN {roles: $roles}]->(m)
                        """,
                        name=name, title=title, roles=roles
                    )
                )
        logging.info("Successfully created Acted_in relationships in Neo4j.")
    except Exception as e:
        logging.error(f"Error creating Acted_in relationships in Neo4j: {e}")
        raise

@app.route(route="HttpExample", auth_level=func.AuthLevel.FUNCTION)
def HttpExample(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        # Delete all nodes in Neo4j
        delete_all_nodes_in_neo4j()

        # Fetch data from PostgreSQL
        person_df, movie_df, acted_in_df = fetch_data_from_postgres()
        
        # Create nodes and relationships in Neo4j
        create_person_nodes_in_neo4j(person_df)
        create_movie_nodes_in_neo4j(movie_df)
        create_acted_in_relationships_in_neo4j(acted_in_df)
        
        return func.HttpResponse("src02 : Person and Movie nodes, and Acted_in relationships created in Neo4j successfully from PostgreSQL data after clearing existing nodes.")
    except Exception as e:
        logging.error(f"Overall error in processing request: {e}")
        return func.HttpResponse("Error creating nodes and relationships in Neo4j.", status_code=500)
