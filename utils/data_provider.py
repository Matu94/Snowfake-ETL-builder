# utils/data_provider.py
import streamlit as st
import pandas as pd
from utils.snowflake_connector import get_session

#Get some sample data for offline dev
class MockDataProvider:
    def get_schemas(self, db_name):
        return["BRONZE", "SILVER", "GOLD"]

    def get_tables(self, schema_name):
        #Returns a fake list of tables for testing UI
        if "BRONZE" in schema_name:
            return ["LANDING_USERS", "LANDING_ORDERS", "RAW_LOGS"]
        elif "SILVER" in schema_name:
            return ["DIM_CUSTOMERS", "FACT_ORDERS"]
        else:
            return ["UNKNOWN_TABLE"]

    def get_columns(self, schema_name, table_name, obj_type):
        #Returns fake columns based on table name
        if "USERS" in table_name:
            return [("ID", "NUMBER"), ("NAME", "VARCHAR"), ("CREATED_AT", "TIMESTAMP")]
        elif "ORDERS" in table_name:
            return [("ORDER_ID", "NUMBER"), ("USER_ID", "NUMBER"), ("AMOUNT", "FLOAT")]
        else:
            return [("COL_1", "VARCHAR"), ("COL_2", "NUMBER")]


#returns real data from snowflake
class RealDataProvider:
    def __init__(self):
        self.session = get_session()

    #Get schemas in the current db
    def get_schemas(self, db_name):
        df = self.session.sql(f"SHOW SCHEMAS IN DATABASE {db_name}").collect()
        schemas = [
                row["name"] 
                for row in df 
                if row["name"] not in ["INFORMATION_SCHEMA", "PUBLIC"] #Optional filtering
            ]
        return schemas

    #Get tables in a specific schema, default is all so don't need to specify in some cases
    def get_tables(self, schema_name, obj_type='all'):
        #1 collect all data
        #maybe use UPPER() later, if someone was stupid enough to name the table with lowercase 
        df_all = self.session.sql(f"SHOW TABLES IN SCHEMA {schema_name}").collect()
        tables_all = [row["name"] for row in (df_all)]
        if obj_type == 'all':
            return tables_all

        #2 collect dt data
        df_dt = self.session.sql(f"SHOW DYNAMIC TABLES IN SCHEMA {schema_name}").collect()
        tables_dt = [row["name"] for row in (df_dt)]
        
        #handle dt/normal
        if obj_type == 'normal':
            return list(set(tables_all) - set(tables_dt))  #Have to use "sets" bc cant substract 1list rom another. INVALID:[1, 2, 3] - [2], VALID:{1, 2, 3} - {2}  
        elif obj_type == 'dynamic':
            return tables_dt

    
    #Get views in a specific schema
    def get_views(self, schema_name):
        df = self.session.sql(f"SHOW VIEWS IN SCHEMA {schema_name}").collect()
        views = [row["name"] for row in df]
        return views

    #Get columns in a specific table/view 
    def get_columns(self, schema_name, obj_name, obj_type):
        if obj_type in ('Table','Dynamic Table'):
            df = self.session.sql(f"DESCRIBE TABLE {schema_name}.{obj_name}").collect()
        elif obj_type == 'View':
            df = self.session.sql(f"DESCRIBE VIEW {schema_name}.{obj_name}").collect()
        columns = [(row["name"], row["type"], row["null?"]) for row in df]
        return columns
    
    #simple DESC command not enough to get the transforms like LEFT(ID,2)
    def get_transform(self, schema_name, obj_name, obj_type):
        if obj_type == 'View':
            df = self.session.sql(f"SELECT GET_DDL('VIEW', '{schema_name}.{obj_name}')").collect()
            

        elif obj_type == 'Dynamic Table':
            df = self.session.sql(f"SELECT GET_DDL('TABLE', '{schema_name}.{obj_name}')").collect()
            

        ddl = df[0][0]  # Extract the DDL string

        # Find the SELECT statement part
        select_start = ddl.upper().find('SELECT')
        from_start = ddl.upper().find('FROM', select_start)

        if select_start != -1 and from_start != -1:
            # Extract the column definitions between SELECT and FROM
            select_clause = ddl[select_start + 6:from_start].strip()

            # Split by comma (handling potential commas in functions)
            columns = []
            paren_depth = 0
            current_col = []

            # If we just split by commas, LEFT(KEK,2) would be incorrectly split into LEFT(KEK and 2)
            # At this point I understand this, but tomorrow i'll need AI again
            for char in select_clause:
                if char == '(':
                    paren_depth += 1
                elif char == ')':
                    paren_depth -= 1
                elif char == ',' and paren_depth == 0:
                    columns.append(''.join(current_col).strip())
                    current_col = []
                    continue
                current_col.append(char)

            # Add the last column
            if current_col:
                columns.append(''.join(current_col).strip())

            # Parse columns with transformations into different variables to be able to re-use them.
            results = []
            for col in columns:
                col_upper = col.upper()
                if ' AS ' in col_upper and '::' in col:
                    # Find the AS keyword position
                    as_pos = col_upper.rfind(' AS ')
                    alias = col[as_pos + 4:].strip()

                    # Everything before AS
                    before_as = col[:as_pos].strip()

                    # Find the :: to split transformation and type
                    type_pos = before_as.rfind('::')
                    transformation = before_as[:type_pos].strip()
                    data_type = before_as[type_pos + 2:].strip()

                    results.append({
                        'alias': alias,
                        'type': data_type,
                        'transformation': transformation
                    })

            return results

        return []
        
    #Helper method for transform, to be able to get the transformation based on the "alias"
    def get_transform_by_alias(self, schema_name, obj_name, obj_type, alias):
        transformations = self.get_transform(schema_name, obj_name, obj_type)
     
        for tf in transformations:
            if tf['alias'].upper() == alias.upper():
                return tf['transformation'].upper()
     
        return None  #or return {'alias': alias, 'type': None, 'transformation': None}
    

    #Returns the source schema and obj name - use this in MODIFIY VIEW
    def get_source(self, schema_name, obj_name, obj_type):
        if obj_type == 'View':
            df = self.session.sql(f"SELECT GET_DDL('VIEW', '{schema_name}.{obj_name}')").collect()
        elif obj_type == 'Dynamic Table':
            df = self.session.sql(f"SELECT GET_DDL('TABLE', '{schema_name}.{obj_name}')").collect()

        ddl = df[0][0]  # Extract the DDL string

        # Find the FROM clause
        from_pos = ddl.upper().find('FROM')

        if from_pos == -1:
            return None, None

        #Extract everything after FROM
        after_from = ddl[from_pos + 4:].strip()

        #Get the first word (the source table/view), stop at semicolon, space, or newline
        source_full = after_from.split(';')[0].split()[0].strip()

        #Split by dot to separate schema and object name
        parts = source_full.split('.')

        if len(parts) == 2:
            obj_source_schema = parts[0]
            obj_source_name = parts[1]
        elif len(parts) == 3:
            # If it includes database name: DATABASE.SCHEMA.TABLE
            obj_source_schema = parts[1]
            obj_source_name = parts[2]
        else:
            return None, None

        return obj_source_schema, obj_source_name
        



    def get_dynamic_table_config(self, schema_name,obj_name):
        df = self.session.sql(f"SELECT GET_DDL('TABLE', '{schema_name}.{obj_name}')").collect()
        ddl = df[0][0] #have to get the "body" part

        #Find target_lag
        target_lag = None
        target_lag_pos = ddl.upper().find('TARGET_LAG')
        if target_lag_pos != -1:
            #Find the opening quote after target_lag =
            quote_start = ddl.find("'", target_lag_pos)
            if quote_start != -1:
                #Find the closing quote
                quote_end = ddl.find("'", quote_start + 1)
                if quote_end != -1:
                    target_lag = ddl[quote_start + 1:quote_end]

        #Find warehouse
        warehouse = None
        warehouse_pos = ddl.upper().find('WAREHOUSE')
        if warehouse_pos != -1:
            #Find the = sign after warehouse
            equals_pos = ddl.find('=', warehouse_pos)
            if equals_pos != -1:
                #et everything after = and extract the warehouse name
                after_equals = ddl[equals_pos + 1:].strip()
                #Split by whitespace and take the first word
                warehouse = after_equals.split()[0].strip()

        return warehouse, target_lag

# Factory function to get the provider
def get_data_provider():
    #if local -> use Mock, if Server -> use Real
    return RealDataProvider()
    #return MockDataProvider()