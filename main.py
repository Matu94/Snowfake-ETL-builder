from models.table import Table


if __name__ == "__main__":
    my_table = Table("raw","employees","id int, name string")
    print(my_table.create_ddl())