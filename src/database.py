import json
from uuid import UUID, uuid4

import psycopg2
import psycopg2.extras


class ReferenceSample:
    def __init__(
        self,
        id: UUID,
        part: int,
        order1: list[list[str]],
        order2: list[list[str]],
        order3: list[list[str]],
        weight: float,
        theme: str,
    ):
        self.id = id
        self.part = part
        self.order1 = order1
        self.order2 = order2
        self.order3 = order3
        self.weight = weight
        self.theme = theme

    def __repr__(self) -> str:
        return f"Sample(id={self.id}, order1={self.order1}, order2={self.order2}, order3={self.order3}, weight={self.weight}, theme={self.theme})"

    def toJSON(self):
        return json.dumps(self.__dict__)


class Database:
    def __init__(
        self, user_name: str, password: str, db_name: str, host: str, port: int
    ):
        psycopg2.extras.register_uuid()
        self.connection = psycopg2.connect(
            dbname=db_name, user=user_name, password=password, host=host, port=port
        )
        self.connection.autocommit = True
        cursor = self.connection.cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS reference_samples (id UUID, part int, order1 TEXT, order2 TEXT, order3 TEXT, weight FLOAT8, theme TEXT, PRIMARY KEY (id, part))"
        )
        cursor.close()
        self.connection.autocommit = False

    def clear_table(self):
        with self.connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE reference_samples")
            self.connection.commit()

    def get_reference_samples(self, theme: str) -> list[ReferenceSample]:
        result = []
        with self.connection.cursor() as cursor:
            query = "SELECT id, part, order1, order2, order3, weight, theme FROM reference_samples WHERE theme=%(theme)s"
            params = {"theme": theme}
            cursor.execute(query, params)

            raw_data = cursor.fetchall()
            for data in raw_data:
                order_1: list[list[str]] = [
                    part.split(",") for part in data[2].split(";")
                ]
                order_2: list[list[str]] = [
                    part.split(",") for part in data[3].split(";")
                ]
                order_3: list[list[str]] = [
                    part.split(",") for part in data[4].split(";")
                ]
                result.append(
                    ReferenceSample(
                        data[0],
                        data[1],
                        order_1,
                        order_2,
                        order_3,
                        data[5],
                        data[6],
                    )
                )
        return result

    def dump_json(self, file_name):
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT id, part, order1, order2, order3, weight, theme FROM reference_samples"
            )
            raw_data = cursor.fetchall()

        def data_sample_to_dict(sample):
            result = dict()
            result["id"] = str(sample[0])
            result["part"] = sample[1]
            result["order1"] = sample[2]
            result["order2"] = sample[3]
            result["order3"] = sample[4]
            result["weight"] = sample[5]
            result["theme"] = sample[6]
            return result

        export_data = list(map(data_sample_to_dict, raw_data))
        with open(file_name, "w") as dump_file:
            json.dump(export_data, dump_file)

    def load_json(self, file_name):
        with open(file_name, "r") as dump_file:
            import_data = json.load(dump_file)
        with self.connection.cursor() as cursor:
            for data in import_data:
                cursor.execute(
                    "INSERT INTO reference_samples (id, part, order1, order2, order3, weight, theme) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id, part) DO UPDATE SET order1=%s, order2=%s, order3=%s, weight=%s, part=%s, theme=%s",
                    (
                        UUID(data["id"]),
                        int(data["part"]),
                        data["order1"],
                        data["order2"],
                        data["order3"],
                        float(data["weight"]),
                        data["theme"],
                        data["order1"],
                        data["order2"],
                        data["order3"],
                        float(data["weight"]),
                        int(data["part"]),
                        data["theme"],
                    ),
                )
            self.connection.commit()

    def insert_new_samples(self, samples: list[ReferenceSample]):
        with self.connection.cursor() as cursor:
            for sample in samples:
                order1 = ";".join(map(",".join, sample.order1))
                order2 = ";".join(map(",".join, sample.order2))
                order3 = ";".join(map(",".join, sample.order3))
                cursor.execute(
                    "INSERT INTO reference_samples (id, part, order1, order2, order3, weight, theme) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id, part) DO UPDATE SET order1=%s, order2=%s, order3=%s, weight=%s, part=%s, theme=%s",
                    (
                        sample.id,
                        sample.part,
                        order1,
                        order2,
                        order3,
                        sample.weight,
                        sample.theme,
                        order1,
                        order2,
                        order3,
                        sample.weight,
                        sample.part,
                        sample.theme,
                    ),
                )
            self.connection.commit()

    def __del__(self):
        self.connection.close()


# if __name__ == "__main__":
#     db = Database("EngineUser", "Bstu31", "testDb", "localhost", 5435)
#     db.clear_table()
#     a = []
#     a.append(
#         ReferenceSample(
#             id=uuid4(),
#             part=0,
#             order1=[["1", "4", "7"], ["4", "5", "6"]],
#             order2=[["2", "5", "8"], ["4", "5", "6"]],
#             order3=[["3", "6", "9"], ["4", "5", "6"]],
#             weight=1,
#         )
#     )
#     a.append(
#         ReferenceSample(
#             id=uuid4(),
#             part=0,
#             order1=[["1", "2", "3"], ["4", "5", "6"]],
#             order2=[["1", "2", "3"], ["4", "5", "6"]],
#             order3=[["1", "2", "3"], ["4", "5", "6"]],
#             weight=1,
#         )
#     )
#     db.insert_new_samples(a)
#     print(db.get_reference_samples())
#     db.dump_json("db_dump.json")
#     db.load_json("db_dump.json")
#     print(db.get_reference_samples())
