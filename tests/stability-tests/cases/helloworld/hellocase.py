from taostest import TDCase, T, runner


class HelloCase(TDCase):
    def init(self):
        pass

    def cleanup(self) -> None:
        pass

    def run(self):
        pass

    def desc(self) -> str:
        return "Hello Case"

    def author(self) -> str:
        return "DingBo"

    def tags(self):
        return T.Query, T.Write.Table.Create, "private-tag1", "private-tag2"

    def get_report(self, start_time, stop_time) -> str:
        return """
        | CPU | Disk | Memory | Thread|
        | ----| ----  |------| -----|
        | 1   |     2 |   3  |   4  |
        """


if __name__ == '__main__':
    runner.run_case("empty.yaml", "helloworld/hellocase.py")
