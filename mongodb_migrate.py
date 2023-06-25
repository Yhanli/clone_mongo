# %%
from pymongo import MongoClient
import os, shutil
import bson.json_util as json_util

from_db_connection_string = (
    "mongodb://root:hougarden@localhost:27011/?authMechanism=DEFAULT"
)
from_db = "RealEstateAU"


to_db_connection_string = (
    "mongodb://root:hougarden@localhost:27010/?authMechanism=DEFAULT"
)


basefolder = "tmp"


def check_make_create_dir(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)
        return True
    return False


from_client = MongoClient(from_db_connection_string)
dbs = from_client.list_database_names()

dbs.pop(dbs.index("admin"))
dbs.pop(dbs.index("config"))
dbs.pop(dbs.index("local"))

print(dbs)
to_client = MongoClient(to_db_connection_string)


def chunk_list(lst, size):
    return [lst[i : i + size] for i in range(0, len(lst), size)]


def write_to_client(db, collection, data):
    db_con = to_client[db]
    coll_con = db_con[collection]
    data_chunks = chunk_list(data, 1000)
    for chunk in data_chunks:
        coll_con.insert_many(chunk)


def migrate():
    for db in dbs:
        db_connection = from_client[db]
        collections = db_connection.list_collection_names()
        for collection in collections:
            path = os.path.join(basefolder, db)
            file_path = os.path.join(path, f"{collection}.json")
            check_make_create_dir(path)
            if not os.path.exists(file_path):
                collection_connection = db_connection[collection]
                data_list = collection_connection.find()
                _json = list(data_list)
                if len(_json):
                    write_to_client(db, collection, _json)
                    with open(file_path, "w+") as fp:
                        fp.write(json_util.dumps(_json))
                    print(collection, len(_json))


if __name__ == "__main__":
    migrate()
