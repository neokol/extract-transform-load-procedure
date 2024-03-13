from flask import Flask, request, jsonify
from pymongo import MongoClient
import json
from bson import json_util
from neo4j import GraphDatabase


app = Flask(__name__)

# MongoDB

def get_mongo_data(start_date:int, end_date:int) -> list:
    """
        Collects the data of band from MongoDB 
        Args: 
            start_date (int): Start date for querying in Unix timestamp format.
            end_date (int): End date for querying in Unix timestamp format.
        Returns:
            A list containing dictionaries with all the documents found by the query.
    """
    client = MongoClient('localhost', 27017)
    db = client['band_db']
    collection = db['bands']

    # Fetch data
    bands = collection.find({"year_created": {"$gte": start_date, "$lte": end_date}})
    bands_list = []
    for band in bands:
        band['_id'] = str(band['_id'])  # Convert ObjectId to string
        bands_list.append(band)

    return bands_list

@app.route('/get_bands', methods=['GET'])
def get_bands():
    """
    Flask route to handle the request for fetching band data from MongoDB.

    Returns:
        A JSON response containing a list of bands created within the given date range.
        If start_date or end_date is not provided, it returns a JSON error message.

    Example request: 
        GET /get_bands?start_date=1990&end_date=2000
    """
    
    # Store dates to varialbles
    start_date = request.args.get('start_date', type=int)
    end_date = request.args.get('end_date', type=int)
    # Validate  dates
    if not start_date or not end_date:
        return jsonify({"error": "Please provide both start_date and end_date"}), 400

    bands_data = get_mongo_data(start_date, end_date)
    return json.dumps(bands_data, default=json_util.default)



# Neo4JDB

class Neo4jConnection:
    """
    A class to manage the connection to a Neo4j database.

    This class encapsulates methods for connecting to a Neo4j database, executing queries,
    and handling the database session.

    Attributes:
        __uri (str): The URI for the Neo4j database.
        __user (str): The username for the Neo4j database.
        __password (str): The password for the Neo4j database.
        __driver: The Neo4j database driver instance.

    Methods:
        close(): Closes the database connection.
        query(query, parameters=None, db=None): Executes a given query on the database.
    """
    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__password = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__password))
        except Exception as e:
            print("Failed to create the driver:", e)
    
    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, parameters=None, db=None):
        """
        Executes a Cypher query against the Neo4j database.

        This method opens a session, runs the specified query, and then closes the session.
        It returns the results of the query as a list.

        Args:
            query (str): The Cypher query to be executed.
            parameters (dict, optional): The parameters to be used with the Cypher query. Defaults to None.
            db (str, optional): The name of the Neo4j database on which to run the query. Defaults to None.

        Returns:
            list: A list of records returned by the query.
        """
        with self.__driver.session(database=db) as session:
            result = session.run(query, parameters)
            return [record for record in result]

# def serialize_node(node):
#     return {"labels": list(node.labels), "properties": dict(node)}



def serialize_node(node):
    return {
        "user_id": node.id,  # Extracting the unique identifier of the node
        "properties": {
            "name": node["name"],
            "favoriteBands": node["favoriteBands"]
        }
    }

# Initialize Neo4j connection
neo4j_conn = Neo4jConnection(uri="bolt://localhost:7687", user="neo4j", pwd="81829192")

@app.route('/get_user_data', methods=['GET'])
def get_user_data():
    user_name = request.args.get('name')
    if not user_name:
        return jsonify({"error": "Please provide a user name"}), 400

    query = """
    MATCH (user:User {name: $name})-[:FRIEND]->(friend:User)
    RETURN user, collect(friend) as friends
    """
    result = neo4j_conn.query(query, parameters={'name': user_name})
    data = []
    for record in result:
        user_data = serialize_node(record["user"])
        friends_data = [serialize_node(friend) for friend in record["friends"]]
        data.append({"user": user_data, "friends": friends_data})

    return jsonify(data)





if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
