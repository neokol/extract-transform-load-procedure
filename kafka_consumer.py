from kafka import KafkaConsumer
import mysql.connector
import json
import configuration

# MySQL Connection Configuration
mydb = mysql.connector.connect(
    host=configuration.MYSQL_HOST,
    user=configuration.MYSQL_USER,
    password=configuration.MYSQL_PASSWORD,
    database=configuration.MYSQL_DATABASE
)

mycursor = mydb.cursor()

# Kafka Consumer Configuration
consumer = KafkaConsumer(
    'bands-topic',
    'users-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

## Process and Insert Data into MySQL
def process_message(message:dict):
    """
        Function to process the message received from the consumer, 
        extracting relevant information and inserting it into a MySQL table.
        
        Parameters:
            - message (dict): The dictionary containing the data extracted from the message.
    """
    print("Received message:", message.value)
    
    
    # Retrieves band information (ID, name, year created) from the message
    if message.topic == 'bands-topic':
        band_data = message.value
        print("Received band data:", band_data)
        band_id = band_data.get('_id')
        print(band_id)
        band_name = band_data.get('name')
        print(band_name)
        year_created = band_data.get('year_created')
        print(year_created)
        # Inserts or updates this information in the bands table.
        insert_band_query = """
            INSERT INTO bands (band_id, band_name, year_created)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
            band_name = VALUES(band_name), year_created = VALUES(year_created);
        """
        mycursor.execute(insert_band_query, (band_id, band_name, year_created))
        mydb.commit()

        # Iterates over albums associated with the band and inserts them into the albums table.
        for album in band_data.get('albums', []):
            # Inserts user information into the users table
            insert_album_query = """
                INSERT INTO albums (band_id, album_title)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE
                album_title = VALUES(album_title);
            """
            mycursor.execute(insert_album_query, (band_id, album))
            mydb.commit()
    elif message.topic == 'users-topic':
        # Extracts user information (ID and name) and their favorite bands from the message.
        print("user-topics starts")
        user_data = message.value['user']
        print(user_data)
        user_id = user_data['user_id']
        print(user_id)
        user_name = user_data['properties'].get('name')
        print(user_name)
        favorite_bands = user_data['properties'].get('favoriteBands', [])
        print(favorite_bands)
        # Insert User data
        insert_user_query = """
            INSERT INTO users (user_id, user_name)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE
            user_name = VALUES(user_name);
        """
        mycursor.execute(insert_user_query, (user_id, user_name))
        mydb.commit()

        # Get the user ID
        # mycursor.execute("SELECT user_id FROM users WHERE user_name = %s", (user_name,))
        # user_id = mycursor.fetchone()[0]

        # Insert User Favorites data
        for band_name in favorite_bands:
            # for each favorite band, finds the corresponding band_id and inserts records into the user_favorites table
            # Example: SELECT band_id FROM bands WHERE band_name = %s
            mycursor.execute("SELECT band_id FROM bands WHERE band_name = %s", (band_name,))
            band_id_result  = mycursor.fetchone()
            print(band_id_result)
            if band_id_result:
                band_id = band_id_result[0]
                print(band_id)
                print(band_id)
            # print(band_id)
            if band_id:
                insert_favorite_query = """
                    INSERT INTO user_favorites (user_id, band_id)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE
                    band_id = VALUES(band_id);
                """
                mycursor.execute(insert_favorite_query, (user_id, band_id))
                mydb.commit()


# Consume Messages
for message in consumer:
    process_message(message)

# Close the database connection
mydb.close()
