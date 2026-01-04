from models.base import DatabaseObject


class Table(DatabaseObject):

    def create_ddl(self):
        # f-strings handle the spacing and variables cleanly
        #ddl = f"CREATE OR REPLACE TABLE {self.schema}.{self.name} ({self.columns})"
        ddl = f"CREATE OR REPLACE TABLE {self.schema}.{self.name}(\n\t{self.columns}\n);"
        return ddl
    
