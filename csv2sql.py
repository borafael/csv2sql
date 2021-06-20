import sys

def clean(line):
    return line.replace('\n', '').replace('\r', '')

def getData(file):
    with open(file) as reader:

        lines = reader.readlines()

        columns = None
        rows = []

        for line in lines:

            if columns == None:
                columns = clean(line).split(',')
            else:
                rows.append(clean(line).split(','))

    return (columns, rows)

def rowToSelect(columns, row):
    select = "SELECT "

    for columnIndex in range(0, len(columns)):
        select = select + "'" + row[columnIndex] + "' AS " + columns[columnIndex].replace(' ', '_')

        if columnIndex < len(columns) - 1:
            select = select + ','

    return select;

def csvToSql(file):
    (columns, rows) = getData(file)

    sql = 'WITH data AS (\n'

    for rowIndex in range(0, len(rows)):
        sql = sql + "\t" + rowToSelect(columns, rows[rowIndex])

        if rowIndex < len(rows) - 1:
            sql = sql + " UNION\n"
        else:
            sql = sql + "\n"

    sql = sql + ") \nSELECT * FROM data;"

    return sql

def main():

    if len(sys.argv) != 2:
        print("Usage: python csv2sql.py <csv file>")

    print(csvToSql(sys.argv[1]))


if __name__ == "__main__":
  main()
