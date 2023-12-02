from __future__ import annotations

import os.path

import constants


class SchemaGenerator:

    def __init__(self, logger, namespace, settings, table):
        self.namespace = namespace
        self.table_name = table

        self._logger = logger
        self._settings = settings

        self.schema_object = None
        self.schema_json = None

    @property
    def table_def(self) -> dict:
        return self.transverse_schema_object(self.schema_object.schema, "properties/value") if self.is_valid_schema_object(self.schema_object) else None

    @property
    def fields(self) -> dict:
        return self.transverse_schema_object(self.table_def, "properties") if self.table_def is not None else None

    @property
    def keys(self) -> dict:
        return self.transverse_schema_object(self.schema_object.schema, "properties/key/properties") if self.is_valid_schema_object(self.schema_object) else None

    @property
    def meta(self) -> dict:
        return self.transverse_schema_object(self.schema_object.schema, "properties/meta/properties") if self.is_valid_schema_object(self.schema_object) else None

    @property
    def required(self) -> list:
        key_list = self.transverse_schema_object(self.schema_object.schema, "properties/key/required")
        field_list = self.transverse_schema_object(self.schema_object.schema, "properties/value/required")

        return key_list if not None else [] + field_list if not None else []

    @staticmethod
    def is_valid_schema_object(schema_object):
        return schema_object is not None and hasattr(schema_object, 'schema') and schema_object.schema is not None

    def transverse_schema_object(self, dictionary, path_string):
        if len(path_string) == 0:
            return dictionary

        path_parts = path_string.split('/')

        if not path_parts:
            return dictionary

        key = path_parts[0]
        remaining_path = '/'.join(path_parts[1:])

        if key in dictionary:
            return self.transverse_schema_object(dictionary[key], remaining_path)
        else:
            return None

    async def initialize(self, session) -> SchemaGenerator:
        self._logger.debug(f"Gathering Schema for {self.table_name}")
        self.schema_object = await session.get_table_schema(self.namespace, self.table_name)

        self._logger.debug(f"Completed Gathering Schema for {self.table_name}")

        return self

    def table_sql(self):
        return f"{self._drop_sql()}{self._table_sql()};\n"

    def _drop_sql(self):
        return f"DROP TABLE IF EXISTS `{self.table_name}`;\n\n"

    def _table_sql(self) -> str:
        sql = f"CREATE TABLE `{self.table_name}`\n(\n  {self.build_columns()}"

        table_constraints = self.build_constraints()
        if len(table_constraints) > 0:
            sql += f",\n{table_constraints}"

        sql += "\n)" # Close the field definitions

        comment = ""
        if "title" in self.table_def:
            comment += self.table_def["title"]

        if "description" in self.table_def:
            comment += f" -> {self.table_def['description']}"

        if comment is not None and len(comment) > 0:
            comment = comment.replace("'", '"')
            sql += f"\nCOMMENT = '{comment}'"

        return sql

    def build_columns(self) -> str:
        sql = []

        columns = {**self.keys,  **self.fields, **self.meta}.items()
        field_name_length = max(len(name) for name, definition in columns) + 4

        [sql.append(self.build_column(name, definition, field_name_length).rstrip(' ')) for name, definition in columns]
        return ",\n  ".join(sql)

    def build_column(self, name, definition, name_length = 25) -> str:
        sql_table_name = f"`{name}`"
        sql = f"{sql_table_name.ljust(name_length, ' ')} {self.field_type(definition).ljust(20, ' ')} {self.null_not_null(name).ljust(11)}"

        comment = ""
        if "description" in definition:
            comment = definition['description']

        enum_values = self.enum_values(definition)
        if len(enum_values) > 0:
            enum_comment = f"{', '.join(enum_values)}"
            comment += f" [{enum_comment}]" if len(comment) > 0 else enum_comment

        if comment is not None and len(comment) > 0:
            comment = comment.replace("'", "''")
            sql += f"COMMENT '{comment}'"

        return sql

    def null_not_null(self, name):
        return f"{'NOT ' if name in self.required else ''}NULL"

    def build_constraints(self) -> str:
        constraints = []

        keys = self.keys
        if keys is not None and len(keys) > 0:
            constraints.append(f"{' '.ljust(4, ' ')}CONSTRAINT {self.table_name}_PK PRIMARY KEY ({', '.join(keys)})")

        return ",  \n".join(constraints)

    @staticmethod
    def handle_switch(key, default_value, switch_dict):
        case_value = switch_dict.get(key)
        return case_value if case_value is not None else default_value

    def field_type(self, definition) -> str:
        case_function = SchemaGenerator.handle_switch(
            definition["type"],
            None,
            {
                "integer": SchemaGenerator.field_type_integer,
                "string": SchemaGenerator.field_type_string,
                "boolean": SchemaGenerator.field_type_boolean,
                "number": SchemaGenerator.field_type_number,
                "array": lambda _: "LONGTEXT",
                "object": lambda _: "LONGTEXT"
            }) if "type" in definition else None

        if case_function is None:
            if "oneOf" in definition:
                case_function = self.handle_one_of
            else:
                print(f"Invalid Type for table {self.table_name} {definition}")

        return_value = "UNKNOWN"
        if case_function is None:
            return "NONE"

        return_value = case_function(definition)
        if return_value is None:
            return "UNKNOWN"

        return return_value

    def handle_one_of(self, root_definition) -> str:
        definition = root_definition['oneOf'] if "oneOf" in root_definition else None

        if definition is None:
            return None

        return SchemaGenerator.field_type_string(definition[-1])

    @staticmethod
    def field_type_boolean(definition) -> str:
        return "TINYINT"

    @staticmethod
    def field_type_number(definition) -> str:
        return SchemaGenerator.handle_switch(
            definition["format"],
            "DECIMAL(63, 30)",
            {
                "float64": "DOUBLE"
            }
        ) if "format" in definition else "DECIMAL(63, 30)"

    @staticmethod
    def field_type_integer(definition) -> str:
        return SchemaGenerator.handle_switch(
            definition["format"],
            "INT",
            {
                "int64": "BIGINT"
            }
        ) if "format" in definition else None

    @staticmethod
    def field_type_string(definition) -> str:
        return SchemaGenerator.handle_switch(
            definition["format"] if "format" in definition else None,
            SchemaGenerator.field_string_definition(definition),
            {
                "date-time": "DATETIME"
            }
        )

    @staticmethod
    def field_string_definition(definition) -> str:

        max_length = None
        if "maxLength" in definition:
            max_length = definition['maxLength']

        enum_values = SchemaGenerator.enum_values(definition) or []
        if max_length is None and len(enum_values) > 0:
            max_length = max(len(value) for value in SchemaGenerator.enum_values(definition))

        if max_length is not None:
            return f"VARCHAR({max_length})"

        return "LONGTEXT"

    @staticmethod
    def enum_values(definition) -> list:
        if "enum" in definition:
            enum_list = definition["enum"]
            return [value for value in enum_list if not value.startswith("_")]
        else:
            return []

    def load_file(self, csv_file) -> str:

        if constants.csv_detail_file not in csv_file or len(csv_file[constants.csv_detail_file]) <= 0:
            return "## File not found - unable to generate load script"

        if constants.csv_detail_headers not in csv_file or len(csv_file[constants.csv_detail_headers]) <= 0:
            return "## No fields defined - unable to generate load script"

        csv_file_abs = os.path.abspath(f"{csv_file[constants.csv_detail_file]}")
        load_sql  = list();

        load_sql.append("load data\n")
        load_sql.append(f"local infile '{csv_file_abs}'\n")
        load_sql.append(f"into table `{self.table_name}` ")
        load_sql.append("fields terminated by ',' ")
        load_sql.append("optionally enclosed by '\"'\n")
        load_sql.append("ignore 1 rows\n")
        load_sql.append("(\n  ")

        fields = [f"`{field.split('.')[1]}`" for field in csv_file[constants.csv_detail_headers]]
        load_sql.append(",\n  ".join(fields))

        load_sql.append("\n);\n\n")

        if constants.csv_detail_row_count in csv_file and csv_file[constants.csv_detail_row_count] is not None:
            load_sql.append(f"# Expecting {self._settings.readable_number(csv_file[constants.csv_detail_row_count])} rows")

        return "".join(load_sql)
