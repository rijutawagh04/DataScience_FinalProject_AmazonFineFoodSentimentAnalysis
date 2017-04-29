import luigi
import mechanicalsoup
import download_foodreview_data
import time
import requests
import gzip
from zipfile import ZipFile
import urllib
from tempfile import mktemp
import os
import numpy as np
import pandas as pd
from IPython.display import display

class ConvertFoodReviewsData(luigi.Task):
	def requires(self):
        return download_foodreview_data.DownloadFoodReviewsData()

    def input(self):
        return luigi.LocalTarget('../data/finefoods.txt')
    def run(self):
		# Paths for the data directory, text file, and compressed file.
		data_directory = os.path.join('..', 'data')
		if not os.path.exists(data_directory):
			print("[ERROR] data directory ('{}') does not exist".format(data_directory))

		INPUT_FILE_NAME = "finefoods.txt"
		OUTPUT_FILE_NAME = "finefoods.csv"

		input_filepath = os.path.join(data_directory, INPUT_FILE_NAME)
		csv_filepath = os.path.join(data_directory, OUTPUT_FILE_NAME)

		header = [
			"product/productId",
			"review/userId",
			"review/profileName",
			"review/helpfulness",
			"review/score",
			"review/time",
			"review/summary",
			"review/text"]

		simple_header = [
			"productId",
			"userId",
			"profileName",
			"helpfulness",
			"score",
			"time",
			"summary",
			"text"]

		infile = open(input_filepath, "rt", encoding="Latin-1")
		csvfile = open(csv_filepath, "wt", encoding="UTF-8")


		# Write a list of fields as a comma-separated row with a pipe (|) as a quote character.
		def write_quoted_fields(csvfile, field_list):
			csvfile.write('|' + field_list[0] + '|')
			for field in field_list[1:]:
				csvfile.write(',|' + field + '|')
			csvfile.write("\n")


		# Write the header line.
		write_quoted_fields(csvfile, simple_header)

		# Useful controls during debugging.
		record_limit = 1000000
		troublesome_records = [] #370, 211557, 226163, 519217, 521382, 525958, 531539

		field_count = len(header)
		line_count = 0
		record_count = 0
		currentLine = []
		for line in infile:
			#print("Processing line: {}".format(line.strip()))
			line_count += 1
			line = line.strip()

			if line == "":
				if len(currentLine) == field_count:
					write_quoted_fields(csvfile, currentLine)
					record_count += 1

					if (record_count+1) in troublesome_records:
						print("[WARN] troublesome record -1: {}".format(currentLine))

					if record_count in troublesome_records:
						print("[WARN] troublesome record: {}".format(currentLine))

					if (record_count-1) in troublesome_records:
						print("[WARN] troublesome record +1: {}".format(currentLine))

				else:
					print("[WARN] current record appears to be incomplete: {}".format(currentLine))
				
				currentLine = []
				continue

			parts = line.split(": ", 1)
			
			# Check to see if the line looks sensible enough to be added.
			if len(parts) == 2:
				# If there are pipe characters in the text (unlikely), replace them with a slash.
				field = parts[1].strip().replace('|', '/')
				currentLine.append(field)
			else:
				# Throw this away - there are junk lines in the raw file, e.g.:
				# review/profileName: Sherry "Tell us about yourself!
				# School Princi...
				print("[WARN] only found {} parts after splitting line: {}".format(len(parts), parts))
				print("[WARN] line was: {}".format(line))
				
			if record_count > record_limit:
				break


		# Write the final record (if it is complete).
		if len(currentLine) == field_count:
			write_quoted_fields(csvfile, currentLine)
			record_count += 1
			
		# Close files.
		infile.close()
		csvfile.close()

		print("Finished - wrote {} lines and {} records.".format(line_count, record_count))


		column_dtypes = {'productId': str, 'userId': str, 'profileName': str, 'helpfulness': str,
						 'score': np.float64, 'time': np.int64, 'summary': str, 'text': str}

		# For this dataset, 'quoting' must be set to QUOTE_ALL (1) and the quotechar to a pipe (|).
		# The problem is that values in some 'text' fields begin with a ", but don't end with one,
		# and many review texts contain commas, unbalanced quotes and apostrophes.
		review_df = pd.read_table(csv_filepath, delimiter=',', encoding="UTF-8", dtype=column_dtypes, 
								  quoting=1, quotechar='|', engine="c", skip_blank_lines=True, 
								  error_bad_lines=False, warn_bad_lines=True)
		# Convert score to an int, since it isn't truly a float.
		review_df['score'] = review_df['score'].astype(int)

		review_df['helpfulness_numerator'] = [x[0] for x in review_df['helpfulness'].str.split('/')]
		review_df['helpfulness_denominator'] = [x[1] for x in review_df['helpfulness'].str.split('/')]
		review_df['date'] = pd.to_datetime(review_df['time'], unit='s')
		del review_df['helpfulness']
		review_df.to_csv("data/Reviews.csv",index=False)
    def output(self):
        #save file to Data directory
        return luigi.LocalTarget('data/Reviews.csv')

#if __name__ == '__main__':
#   luigi.run()
