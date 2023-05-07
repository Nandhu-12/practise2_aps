import pymongo
import pandas as pd
import json

# Provide the mongodb localhost url to connect python to mongodb.
client = pymongo.MongoClient("mongodb://localhost:27017/neurolabDB")

db_name = "aps2"
col_name = "sensor2"
data_file_path = "/config/workspace/aps_failure_training_set1.csv"


if __name__ == "__main__":
    # loading the data in neurolab
    df = pd.read_csv(data_file_path)
    print(f"shape of df : {df.shape}")

    # converting to json by dropping index col
    df.reset_index(drop=True,inplace=True)
    records = list(json.loads(df.T.to_json()).values())

    print(records[0])

    #loading df into mongodb
    client[db_name][col_name].insert_many(records)

