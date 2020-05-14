if __name__ == "__main__":

  import csv
  import sys

  filename = sys.argv[1]

  rows = 0

  # reading csv file 
  with open(filename, 'r') as csvfile:

    # creating a csv reader object 
    csvreader = csv.reader(csvfile) 

    # extracting field names through first row 
    fields = next(csvreader) 

    # extracting each data row one by one 

    for row in csvreader:
      rows+=1

  print(rows)