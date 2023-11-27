import json
import psycopg2
from uuid import UUID, uuid4 
import psycopg2.extras
    
class DatabaseLegacy:
    def __init__(self, user_name: str, password: str, db_name: str, host: str, port: int):
        self.connection = psycopg2.connect(dbname=db_name, user=user_name, password=password, host=host, port=port)
        self.connection.autocommit = True
        cursor = self.connection.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS reference_samples (id TEXT PRIMARY KEY, order1 TEXT, order2 TEXT, order3 TEXT, weight FLOAT8)")
        cursor.close()
        self.connection.autocommit = False

    def clear_table(self):
        with self.connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE reference_samples")
            self.connection.commit()

    def get_reference_samples(self) -> dict:
        result = dict()
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT id, order1, order2, order3, weight FROM reference_samples")
            raw_data = cursor.fetchall()
            for data in raw_data:
                order_1 = [part.split(",") for part in data[1].split(";")]
                order_2 = [part.split(",") for part in data[2].split(";")]
                order_3 = [part.split(",") for part in data[3].split(";")]
                result[data[0]] = [order_1, order_2, order_3]
        return result
    
    def get_reference_samples_scuffed(self) -> list[dict]:
        result = []
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT id, order1, order2, order3, weight FROM reference_samples")
            raw_data = cursor.fetchall()
            for data in raw_data:
                result.append({"id": data[0], "order1": data[1], "order2": data[2], "order3": data[3], "weight": data[4]})
        return result

    def insert_new_samples(self, samples: list[dict]):
        with self.connection.cursor() as cursor:
            for sample in samples:
                cursor.execute("INSERT INTO reference_samples (id, order1, order2, order3, weight) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id) DO UPDATE SET order1=%s, order2=%s, order3=%s, weight=%s", (sample["id"], sample["order1"], sample["order2"], sample["order3"], sample["weight"], sample["order1"], sample["order2"], sample["order3"], sample["weight"]))
            self.connection.commit()

    def load_json_data(self, json_file_name):
        with open(json_file_name, "r", encoding="utf-8") as file:
            json_data = json.load(file)
            self.insert_new_samples(json_data)

    def __del__(self):
        self.connection.close()

class ReferenceSample():
    def __init__(self, id: UUID, part: int, order1: list[list[str]], order2: list[list[str]], order3: list[list[str]], weight: float):
        psycopg2.extras.register_uuid()
        self.id = id
        self.part = part
        self.order1 = order1
        self.order2 = order2
        self.order3 = order3
        self.weight = weight

    def __repr__(self) -> str:
        return f"Sample(id={self.id}, order1={self.order1}, order2={self.order2}, order3={self.order3}, weight={self.weight})"

    def toJSON(self):
        return json.dumps(self.__dict__)
    

class Database:
    def __init__(self, user_name: str, password: str, db_name: str, host: str, port: int):
        self.connection = psycopg2.connect(dbname=db_name, user=user_name, password=password, host=host, port=port)
        self.connection.autocommit = True
        cursor = self.connection.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS reference_samples (id UUID, part int, order1 TEXT, order2 TEXT, order3 TEXT, weight FLOAT8, PRIMARY KEY(id, part))")
        cursor.close()
        self.connection.autocommit = False

    def clear_table(self):
        with self.connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE reference_samples")
            self.connection.commit()

    def get_reference_samples(self) -> list[ReferenceSample]:
        result = []
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT id, part, order1, order2, order3, weight FROM reference_samples")
            raw_data = cursor.fetchall()
            for data in raw_data:
                order_1: list[list[str]] = [part.split(",") for part in data[2].split(";")]
                order_2: list[list[str]] = [part.split(",") for part in data[3].split(";")]
                order_3: list[list[str]] = [part.split(",") for part in data[4].split(";")]
                result.append(ReferenceSample(data[0],data[1],order_1,order_2,order_3,data[5]))
        return result

    def insert_new_samples(self, samples: list[ReferenceSample]):
        with self.connection.cursor() as cursor:
            for sample in samples:
                order1 = ";".join(map(",".join, sample.order1))
                order2 = ";".join(map(",".join, sample.order2))
                order3 = ";".join(map(",".join, sample.order3))
                cursor.execute("INSERT INTO reference_samples (id, part, order1, order2, order3, weight) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (id, part) DO UPDATE SET order1=%s, order2=%s, order3=%s, weight=%s, part=%s",
                               (sample.id,
                                sample.part,
                                order1,
                                order2,
                                order3,
                                sample.weight,
                                order1,
                                order2,
                                order3,
                                sample.weight,
                                sample.part))
            self.connection.commit()

    def __del__(self):
        self.connection.close()

if __name__ == "__main__":
    db = Database("postgres", "password", "postgres", "localhost", 5432)
    db.clear_table()
    a = []
    a.append(ReferenceSample(
        id=uuid4(),
        part=0,
        order1=[["1", "4", "7"], ["4", "5", "6"]],
        order2=[["2", "5", "8"], ["4", "5", "6"]],
        order3=[["3", "6", "9"], ["4", "5", "6"]],
        weight=1
        ))
    a.append(ReferenceSample(
        id=uuid4(),
        part=0,
        order1=[["1", "2", "3"], ["4", "5", "6"]],
        order2=[["1", "2", "3"], ["4", "5", "6"]],
        order3=[["1", "2", "3"], ["4", "5", "6"]],
        weight=1
        ))
    db.insert_new_samples(a)
    print(db.get_reference_samples())
    # db.load_json_data("test_db.json")
    # print(db.get_reference_samples())
    # print(db.get_reference_samples())
    # data = []
    # data.append({"id": "1234", "order1": "asda", "order2": "dhtye", "order3": "mfgk", "weight": 0.375})
    # data.append({"id": "123", "order1": "asda", "order2": "dhtye", "order3": "mfgk", "weight": 0.875})
    # data.append({"id": "1234", "order1": "asda", "order2": "dhtye", "order3": "mfgk", "weight": 0.675})
    # db.insert_new_samples(data)
    # print(db.get_reference_samples())
    # print(db.get_reference_samples_scuffed())

