ETL Mechanism Project


Overview

In this project, we develop an Extract Transform Load (ETL) mechanism designed to integrate and process data from two distinct types of databases (DBs), and subsequently store the processed data in a third DB. The core functionality is encapsulated within a REST service, enabling effective monitoring and interaction with the system.

Features

Data Retrieval: Extracts data from two different databases, MongoDB and Neo4j.
Data Processing: Integrates and processes the extracted data.
Data Storage: Loads the processed data into a MySQL database.
REST Service: Operates as a REST service built with the Flask Framework, facilitating data retrieval and storage operations through REST requests.
Monitoring: Enables monitoring of the ETL mechanism through the REST service.
Technologies
Programming Language: Python 3
Databases:
Source Databases: MongoDB and Neo4j
Destination Database: MySQL
Web Framework: Flask Framework
Data Integration: Apache Kafka
