import csv


class CSVLoader:
    def __init__(self, file_name):
        self.headers = None
        self.header_row = None
        self.reader = None
        self.file = None
        self.file_name = file_name

        self.open_file()

    def open_file(self):
        self.file = open(self.file_name)
        self.reader = csv.reader(self.file)

    def read_header(self):
        self.header_row = next(self.reader)
        self.headers = []

        for index, column_header in enumerate(self.header_row):
            print(index, column_header)
            self.headers.append(column_header)

    def read_lines(self):
        for row in self.reader:
            for index in range(len(self.headers)):
                print(row[index])

csv_loader = CSVLoader('C:\\code\\pythoncrashcourse\\ehmatthes-pcc_2e-078318e\\chapter_16\\the_csv_file_format\\data\\sitka_weather_07-2018_simple.csv')
csv_loader.read_header()
csv_loader.read_lines()
