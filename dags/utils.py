import csv
import tarfile
import pandas as pd
import sqlite3

def unzip_tolldata(source, destination):
    """
    Extracts the contents of the source dataset .tgz file to the specified
    destination directory.

    Args:
        source (str): Path to the source .tgz file.
        destination (str): Directory where the contents will be extracted.
    """
    try:
        with tarfile.open(source, "r:gz") as tgz:
            tgz.extractall(destination)
    except Exception as e:
        print(f"Error extracting {source}: {e}")

def extract_csv_data(infile, outfile):
    """
    Extracts the specified columns from an input CSV file and saves the result
    to an output CSV file.

    Args:
        infile (str): Path to the input CSV file.
        outfile (str): Path to the output CSV file.
    """
    try:
        with open(infile, "r") as readfile, open(outfile, "w") as writefile:
            for line in readfile:
                # Split the line by comma and
                # select columns 1 to 4 (0-based index)
                selected_columns = ",".join(line.strip().split(",")[:4])
                writefile.write(selected_columns + "\n")
    except Exception as e:
        print(f"Error processing {infile}: {e}")

def extract_tsv_data(infile, outfile):
    """
    Extracts the specified columns from an input TSV file and saves the result
    to an output CSV file.

    Args:
        infile (str): Path to the input TSV file.
        outfile (str): Path to the output CSV file.
    """
    try:
        with open(infile, "r") as readfile, open(outfile, "w") as writefile:
            for line in readfile:
                # Split the line by tab and
                # select columns 5 to 7 (0-based index)
                selected_columns = ",".join(line.strip().split("\t")[4:7])
                writefile.write(selected_columns + "\n")
    except Exception as e:
        print(f"Error processing {infile}: {e}")

def extract_fixed_width_data(infile, outfile):
    """
    Extracts the specified columns from an input fixed width file and
    saves the result to an output CSV file.

    Args:
        infile (str): Path to the input fixed width file.
        outfile (str): Path to the output CSV file.
    """
    try:
        with open(infile, "r") as readfile, open(outfile, "w") as writefile:
            for line in readfile:
                # Remove extra spaces and split by space
                cleaned_line = " ".join(line.split())

                # Select columns 10 and 11 (0-based index) directly
                selected_columns = cleaned_line.split(" ")[9:11]
                writefile.write(",".join(selected_columns) + "\n")
    except Exception as e:
        print(f"Error processing {infile}: {e}")

def consolidate_data_extracted(infile, outfile):
    """
    Combine data from the specified files into a single CSV file.

    Args:
        infile (list): List of input CSV file paths.
        outfile (str): Path to the output CSV file.
    """
    try:
        combined_csv = pd.concat([pd.read_csv(f) for f in infile], axis=1)
        combined_csv.to_csv(outfile, index=False)
    except Exception as e:
        print(f"Error processing {infile}: {e}")

def transform_load_data(infile, outfile):
    """
    Transform the fourth column in a CSV file to uppercase

    Args:
        infile (str): Path to the input CSV file.
        outfile (str): Path to the output CSV file.
    """
    try:
        with open(infile, "r") as readfile, open(outfile, "w") as writefile:
            reader = csv.reader(readfile)
            writer = csv.writer(writefile)
            
            for row in reader:
                # Modify the fourth field (index 3) and convert to uppercase
                row[3] = row[3].upper()
                writer.writerow(row)

    except Exception as e:
        print(f"Error processing {infile}: {e}")

def load_data_to_sqlite(transformed_data, db_name):
    """
    Load transformed data from a CSV file into a SQLite database table

    Args:
        transformed_data (str): Path to the input CSV file.
        db_name (str): Database name.
    """  
    # Sample data loading function
    df = pd.read_csv(transformed_data, header=None)

    # Create connection to SQLite
    conn = sqlite3.connect(db_name)

    # Load data to SQLite
    df.to_sql(name='toll_data', con=conn, if_exists='replace', index=False)
    conn.close()

def query_and_print_data(db_name):
    """
    Query and print data from SQLite

    Args:
        db_name (str): Database name.
    """  
    try:
        # Connect to SQLite and execute query
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM toll_data LIMIT 10")
        
        # Fetch all rows
        rows = cursor.fetchall()
        print("Printing data from SQLite:")
        for row in rows:
            print(row)
        
        conn.close()

    except Exception as e:
        print(f"Error querying data from SQLite: {e}")
