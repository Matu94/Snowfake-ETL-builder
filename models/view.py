from models.base import DatabaseObject


class View(DatabaseObject):

    def create_ddl(self):
        # We assume 'self.columns' holds the full query body here
        ddl = f"CREATE OR REPLACE VIEW {self.schema}.{self.name} AS {self.columns}"
        return ddl