from pathlib import Path
import csv


class CsvHandler:
    def __init__(self, path):
        self._csv_path: Path = Path(path)
        self._lines: list[list[str]] = []
        with open(path, newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            headers = next(reader)
            self._headers = {el: idx for idx, el in enumerate(headers)}
            for row in reader:
                self._lines.append(row)

    def content(self):
        return self._lines

    def header(self):
        return self._headers

