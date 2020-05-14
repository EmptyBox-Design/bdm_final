if __name__ == "__main__":

  import csv
  import sys

  filename = sys.argv[1]

  rows = []

  # reading csv file 
  with open(filename, 'r') as csvfile:

    # creating a csv reader object 
    csvreader = csv.reader(csvfile) 

    # extracting field names through first row 
    fields = next(csvreader) 

    # extracting each data row one by one 
    i = 0
    for row in csvreader:
      i+=1

      rows.append(row)

      if(i == 120000):
        # name of csv file  
        filename = "edit_cscl.csv"
            
        # writing to csv file  
        with open(filename, 'w', newline='') as csvfile:  
            # creating a csv writer object  
            csvwriter = csv.writer(csvfile)  
                
            # writing the fields  
            csvwriter.writerow(fields)  
                
            # writing the data rows  
            csvwriter.writerows(rows)