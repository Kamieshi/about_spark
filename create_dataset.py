from __future__ import annotations
import csv
import datetime
from typing import List, Any, Dict
from psycopg2._psycopg import cursor as cursor_type


class TestData:
    def __init__(self, level: str, data: str) -> None:
        self.level = level
        self.data = data
        self.time = datetime.datetime.now()

    @property
    def get_tuple(self):
        return (self.time, self.level, self.data)


class DataSet:
    def __init__(self):
        self.__data_set: List[TestData] = list()

    def add(self, level, data: str) -> None:
        self.__data_set.append(TestData(level, data))

    def to_csv(self, name_file: str) -> None:
        with open("/home/dmitryrusack/PycharmProjects/SparkArticle/" + name_file + '.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerows([data.get_tuple for data in self.__data_set])

    def to_db(self, cursor: cursor_type) -> None:
        base_sql = "INSERT INTO test(update_time,level,data) VALUES (%s,%s,%s)"
        data = [data.get_tuple for data in self.__data_set]
        for d in data:
            cursor.execute(base_sql, d)

    def to_list_dict(self) -> List[Dict[str, Any]]:
        return [{"datetime": value.time, "level": value.level, "pay_load": value.data} for value in self.__data_set]

    @staticmethod
    def create_dataset(message_types: List[str], count: int) -> DataSet:
        new_ds = DataSet()
        for type in message_types:
            for i in range(1, count + 1):
                new_ds.add(type, f"Data {i}")
        return new_ds

if __name__ == '__main__':
    d_s = DataSet.create_dataset(["ERROR","INFO"],1000)
    d_s.to_csv("test_file")
