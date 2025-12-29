from abc import ABC, abstractmethod


class DatabaseObject(ABC):

    def __init__(self, schema, name, columns):
        self.schema = schema
        self.name = name
        self.columns = columns

    @abstractmethod
    def create_ddl(self):
        pass