import os
import glob
import csv


def prepare_data():
    # Get current folder and subfolder event data
    filepath = os.getcwd() + '/event_data'

    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):
        # Join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root,'*'))

    # Initiate an empty list of rows that will be generated from each file
    full_data_rows_list = []

    for f in file_path_list:
        # Read csv file
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile:
            # Create a csv reader object
            csvreader = csv.reader(csvfile)
            next(csvreader)

            # Extract each data row one by one and append it
            for line in csvreader:
                full_data_rows_list.append(line)

    # Get total number of rows
    print('Total number of rows:', len(full_data_rows_list))

    # Create a smaller event_datafile_full csv file that will be used to
    # insert data into the Apache Cassandra tables
    csv.register_dialect(
        'myDialect',
        quoting=csv.QUOTE_ALL,
        skipinitialspace=True)

    with open(
        'event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow([
            'artist',
            'firstName',
            'gender',
            'itemInSession',
            'lastName',
            'length',
            'level',
            'location',
            'sessionId',
            'song',
            'userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((
                row[0],
                row[2],
                row[3],
                row[4],
                row[5],
                row[6],
                row[7],
                row[8],
                row[12],
                row[13],
                row[16]))

def main():
    prepare_data()


if __name__ == "__main__":
    main()
