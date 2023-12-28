import struct
import sys

META_COMMAND_SUCCESS = 0
META_COMMAND_UNRECOGNIZED_COMMAND = 1

PREPARE_SUCCESS = 0
PREPARE_SYNTAX_ERROR = 1
PREPARE_UNRECOGNIZED_STATEMENT = 2

EXECUTE_SUCCESS = 0
EXECUTE_TABLE_FULL = 1

ID_SIZE = 4
USERNAME_SIZE = 32
EMAIL_SIZE = 255
ID_OFFSET = 0
USERNAME_OFFSET = ID_OFFSET + ID_SIZE
EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE
ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE

PAGE_SIZE = 4096
TABLE_MAX_PAGES = 100
ROWS_PER_PAGE = PAGE_SIZE // ROW_SIZE
TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES


class Row:
    def __init__(self, id, username, email):
        self.id = id
        self.username = username
        self.email = email


class Table:
    def __init__(self):
        self.num_rows = 0
        self.pages = [None] * TABLE_MAX_PAGES


def print_row(row):
    print(f"({row.id}, {row.username}, {row.email})")


def serialize_row(source, destination):
    struct.pack_into("I32s255s", destination, 0, source.id, source.username.encode("utf-8"), source.email.encode("utf-8"))


def deserialize_row(source, destination):
    (destination.id, username_bytes, email_bytes) = struct.unpack_from("I32s255s", source)
    destination.username = username_bytes.decode("utf-8").rstrip("\x00")
    destination.email = email_bytes.decode("utf-8").rstrip("\x00")


def row_slot(table, row_num):
    page_num = row_num // ROWS_PER_PAGE
    page = table.pages[page_num]
    if page is None:
        page = table.pages[page_num] = bytearray(PAGE_SIZE)
    row_offset = row_num % ROWS_PER_PAGE
    byte_offset = row_offset * ROW_SIZE
    return memoryview(page)[byte_offset:byte_offset + ROW_SIZE]


def new_table():
    return Table()


def free_table(table):
    for page in table.pages:
        if page is not None:
            del page
    del table


class InputBuffer:
    def __init__(self):
        self.buffer = None


def print_prompt():
    sys.stdout.write("db > ")
    sys.stdout.flush()


def read_input(input_buffer):
    input_buffer.buffer = input()


def do_meta_command(input_buffer, table):
    if input_buffer.buffer == ".exit":
        free_table(table)
        sys.exit(0)
    else:
        return META_COMMAND_UNRECOGNIZED_COMMAND


def prepare_statement(input_buffer, statement):
    if input_buffer.buffer.startswith("insert"):
        statement.type = "INSERT"
        args = input_buffer.buffer.split()
        if len(args) < 4:
            return PREPARE_SYNTAX_ERROR
        statement.row_to_insert = Row(int(args[1]), args[2], args[3])
        return PREPARE_SUCCESS
    elif input_buffer.buffer == "select":
        statement.type = "SELECT"
        return PREPARE_SUCCESS
    else:
        return PREPARE_UNRECOGNIZED_STATEMENT


def execute_insert(statement, table):
    if table.num_rows >= TABLE_MAX_ROWS:
        return EXECUTE_TABLE_FULL

    row_to_insert = statement.row_to_insert
    serialize_row(row_to_insert, row_slot(table, table.num_rows))
    table.num_rows += 1

    return EXECUTE_SUCCESS


def execute_select(statement, table):
    for i in range(table.num_rows):
        row = Row(0, "", "")
        deserialize_row(row_slot(table, i), row)
        print_row(row)
    return EXECUTE_SUCCESS


def execute_statement(statement, table):
    if statement.type == "INSERT":
        return execute_insert(statement, table)
    elif statement.type == "SELECT":
        return execute_select(statement, table)


def main():
    table = new_table()
    input_buffer = InputBuffer()

    while True:
        print_prompt()
        read_input(input_buffer)

        if input_buffer.buffer[0] == ".":
            if do_meta_command(input_buffer, table) == META_COMMAND_SUCCESS:
                continue

        statement = {"type": None, "row_to_insert": None}
        prepare_result = prepare_statement(input_buffer, statement)

        if prepare_result == PREPARE_SUCCESS:
            execute_result = execute_statement(statement, table)
            if execute_result == EXECUTE_SUCCESS:
                print("Executed.")
            elif execute_result == EXECUTE_TABLE_FULL:
                print("Error: Table full.")
        elif prepare_result == PREPARE_SYNTAX_ERROR:
            print("Syntax error. Could not parse statement.")
        elif prepare_result == PREPARE_UNRECOGNIZED_STATEMENT:
            print(f"Unrecognized keyword at start of '{input_buffer.buffer}'.")


if __name__ == "__main__":
    main()
