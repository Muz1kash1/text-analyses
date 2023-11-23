import psycopg2
from uuid import UUID 
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
                result[data[0]] = [data[1], data[2], data[3], data[4]]
        return result

    def insert_new_samples(self, samples: dict):
        with self.connection.cursor() as cursor:
            for key, value in samples.items():
                cursor.execute("INSERT INTO reference_samples (id, order1, order2, order3, weight) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id) DO UPDATE SET order1=%s, order2=%s, order3=%s, weight=%s", (key, value[0], value[1], value[2], value[3], value[0], value[1], value[2], value[3]))
            self.connection.commit()

    def __del__(self):
        self.connection.close()

class ReferenceSamples():
    def __init__(self, id: UUID, order1: str, order2: str, order3: str, weight: float):
        psycopg2.extras.register_uuid()
        self.id = id
        self.order1 = order1
        self.order2 = order2
        self.order3 = order3
        self.weight = weight

    def __repr__(self) -> str:
        return f"Sample(id={self.id}, order1={self.order1}, order2={self.order2}, order3={self.order3}, weight={self.weight})"


class Database:
    def __init__(self, user_name: str, password: str, db_name: str, host: str, port: int):
        self.connection = psycopg2.connect(dbname=db_name, user=user_name, password=password, host=host, port=port)
        self.connection.autocommit = True
        cursor = self.connection.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS reference_samples (id UUID PRIMARY KEY, order1 TEXT, order2 TEXT, order3 TEXT, weight FLOAT8)")
        cursor.close()
        self.connection.autocommit = False

    def clear_table(self):
        with self.connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE reference_samples")
            self.connection.commit()

    def get_reference_samples(self) -> list[ReferenceSamples]:
        result = []
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT id, order1, order2, order3, weight FROM reference_samples")
            raw_data = cursor.fetchall()
            for data in raw_data:
                result.append(ReferenceSamples(data[0],data[1],data[2],data[3],data[4]))
        return result

    def insert_new_samples(self, samples: list[ReferenceSamples]):
        with self.connection.cursor() as cursor:
            for sample in samples:
                cursor.execute("INSERT INTO reference_samples (id, order1, order2, order3, weight) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id) DO UPDATE SET order1=%s, order2=%s, order3=%s, weight=%s", (sample.id, sample.order1, sample.order2, sample.order3, sample.weight, sample.order1, sample.order2, sample.order3, sample.weight))
            self.connection.commit()

    def __del__(self):
        self.connection.close()

if __name__ == "__main__":
    db = DatabaseLegacy("postgres", "password", "postgres", "localhost", 5432)
    db.clear_table()
    data = dict()
    data["1234"] = ["bruh", "kwuh", "jmih", 0.765]
    data["1234"] = ["bruh", "kwuh", "jmih", 0.365]
    data["12"] = ["bruh", "kwuh", "jmih", 0.765]
    db.insert_new_samples(data)
    print(db.get_reference_samples())

