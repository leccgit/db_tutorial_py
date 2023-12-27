from enum import IntEnum


class StopInput(Exception):
    pass


class MetaCommandResult(IntEnum):
    META_COMMAND_SUCCESS = 1
    META_COMMAND_UNRECOGNIZED_COMMAND = 2


class StatementType(IntEnum):
    STATEMENT_INSERT = 1
    STATEMENT_SELECT = 2


class PrepareResult(IntEnum):
    PREPARE_SUCCESS = 1
    PREPARE_UNRECOGNIZED_STATEMENT = 2


class Statement:
    def __init__(self):
        self.type = None


def do_meta_command(input_text: str) -> int:
    return MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND


def prepare_statement(input_text: str, statement: Statement) -> int:
    if input_text == 'insert':
        statement.type = StatementType.STATEMENT_INSERT
        return PrepareResult.PREPARE_SUCCESS
    elif input_text == 'select':
        statement.type = StatementType.STATEMENT_SELECT
        return PrepareResult.PREPARE_SUCCESS
    return PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT


def execute_statement(statement: Statement):
    state_type = statement.type
    if state_type == StatementType.STATEMENT_INSERT:
        print("This is where we would do an insert.\n")
    elif state_type == StatementType.STATEMENT_SELECT:
        print("This is where we would do a select.\n")


def main():
    while True:
        input_text = print_prompt()
        if len(input_text) == 0:
            print('Error reading input\n')
            break

        if input_text[0] == '.':
            if input_text == ".exit":
                print("Exiting program.")
                break
            command = do_meta_command(input_text)
            if command == MetaCommandResult.META_COMMAND_SUCCESS:
                pass
            elif command == MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND:
                print(f"Unrecognized command '{input_text}'.")
        statement = Statement()
        prepare_result = prepare_statement(input_text, statement)
        if prepare_result == PrepareResult.PREPARE_SUCCESS:
            pass
        else:
            print(f"Unrecognized keyword at start of '{input_text}'.\n")

        execute_statement(statement)
        print("Executed.\n")


def print_prompt():
    return input("db > ")


if __name__ == '__main__':
    main()
