import subprocess
import unittest


class TestDatabase(unittest.TestCase):
    def run_script(self, commands):
        raw_output = None
        with subprocess.Popen(["python", "db_tutorial.py"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                              text=True) as pipe:
            for command in commands:
                pipe.stdin.write(command + "\n")

            pipe.stdin.close()

            # Read entire output
            raw_output = pipe.stdout.read()

        return raw_output.split("\n")

    def test_insert_and_retrieve_row(self):
        result = self.run_script([
            "insert 1 user1 person1@example.com",
            "select",
            ".exit",
        ])

        expected_output = [
            "db > Executed.",
            "db > (1, user1, person1@example.com)",
            "Executed.",
            "db > ",
        ]

        self.assertEqual(result, expected_output)

    def test_insert_maximum_length(self):
        long_user_name = 'a' * 32
        long_email = 'a' * 256
        result = self.run_script([
            f"insert 1 {long_user_name} {long_email}",
            "select",
            ".exit",
        ])

        print(result)
        expected_output = [
            "db > Executed.",
            f"db > (1, {long_user_name}, {long_email})",
            "Executed.",
            "db > ",
        ]
        print(expected_output)

        self.assertEqual(result, expected_output)


if __name__ == "__main__":
    unittest.main()
