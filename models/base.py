from abc import ABC, abstractmethod


class DatabaseObject(ABC):

    def __init__(self, name, schema, columns):
        self.name = name
        self.schema = schema
        self.columns = columns

    @abstractmethod
    def create_ddl(self):
        pass