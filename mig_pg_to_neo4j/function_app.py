import azure.functions as func
import logging
import os
from neo4j import GraphDatabase
import pandas as pd
from sqlalchemy import create_engine
from decimal import Decimal
from datetime import datetime

app = func.FunctionApp()

@app.route(route="HttpTrigger_pg_to_neo4j", auth_level=func.AuthLevel.ANONYMOUS)

def HttpTrigger_pg_to_neo4j(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('')
    logging.info('----------------------------------------------------------------------')
    logging.info(f'{datetime.now()} - Python HTTP trigger function processed a request.')
    logging.info('----------------------------------------------------------------------')
    logging.info('')

    try:
        # Delete all nodes in Neo4j
        delete_all_nodes_in_neo4j()

        # Fetch data from PostgreSQL
        person_df, movie_df, acted_in_df = fetch_data_from_postgres()
        
        # Create nodes and relationships in Neo4j
        create_nodes_and_relationships_in_neo4j(person_df, movie_df, acted_in_df)
        
        return func.HttpResponse("src04: Person and Movie nodes, and Acted_in relationships created in Neo4j successfully from PostgreSQL data after clearing existing nodes.")
    except Exception as e:
        logging.error(f"{datetime.now()} - Overall error in processing request: {e}")
        return func.HttpResponse("Error creating nodes and relationships in Neo4j.", status_code=500)



# Configure logging to include timestamp
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Neo4j and PostgreSQL connection details from environment variables
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_DATABASE = "secdb"
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# SQLAlchemy engine for PostgreSQL
pg_connection_string = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
pg_engine = create_engine(pg_connection_string)

# Neo4j driver
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def delete_all_nodes_in_neo4j():
    try:
        with driver.session(database=NEO4J_DATABASE) as session:
            session.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE n"))
        logging.info(f"{datetime.now()} - Successfully deleted all nodes in Neo4j.")
    except Exception as e:
        logging.error(f"{datetime.now()} - Error deleting all nodes in Neo4j: {e}")
        raise

def fetch_data_from_postgres():
    try:
        # Load data from PostgreSQL into DataFrames
        person_df = pd.read_sql("SELECT name, born FROM person", pg_engine)
        movie_df = pd.read_sql("SELECT title, released, tagline FROM movie", pg_engine)
        acted_in_df = pd.read_sql("SELECT name, title, roles FROM acted_in", pg_engine)
        
        logging.info(f"{datetime.now()} - Successfully fetched data from PostgreSQL.")
        return person_df, movie_df, acted_in_df
    except Exception as e:
        logging.error(f"{datetime.now()} - Error fetching data from PostgreSQL: {e}")
        raise

def create_nodes_and_relationships_in_neo4j(person_df, movie_df, acted_in_df):
    try:
        with driver.session(database=NEO4J_DATABASE) as session:
            # Create Person nodes
            for _, row in person_df.iterrows():
                name, born = row['name'], row['born']
                if isinstance(born, Decimal):
                    born = int(born)
                session.execute_write(
                    lambda tx: tx.run(
                        "CREATE (p:Person {name: $name, born: $born})",
                        name=name, born=born
                    )
                )
            
            # Create Movie nodes
            for _, row in movie_df.iterrows():
                title, released, tagline = row['title'], row['released'], row['tagline']
                if isinstance(released, Decimal):
                    released = int(released)
                session.execute_write(
                    lambda tx: tx.run(
                        "CREATE (m:Movie {title: $title, released: $released, tagline: $tagline})",
                        title=title, released=released, tagline=tagline
                    )
                )
            
            # Create Acted_in relationships
            for _, row in acted_in_df.iterrows():
                name, title, roles = row['name'], row['title'], row['roles']
                session.execute_write(
                    lambda tx: tx.run(
                        """
                        MATCH (p:Person {name: $name})
                        MATCH (m:Movie {title: $title})
                        CREATE (p)-[:ACTED_IN {roles: $roles}]->(m)
                        """,
                        name=name, title=title, roles=roles
                    )
                )
                
        logging.info(f"{datetime.now()} - Successfully created Person and Movie nodes, and Acted_in relationships in Neo4j.")
    except Exception as e:
        logging.error(f"{datetime.now()} - Error creating nodes and relationships in Neo4j: {e}")
        raise


