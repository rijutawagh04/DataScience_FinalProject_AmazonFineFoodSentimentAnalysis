import gzip
import luigi
import numpy as np
import os
import pandas as pd
import requests
import sys
from luigi import configuration, s3
from luigi.s3 import S3Target, S3Client

from boto.s3.key import Key
from boto.s3.connection import S3Connection

# URL for the file download.
gzip_file_url = "https://snap.stanford.edu/data/finefoods.txt.gz"

# Paths for the data directory, text file, and compressed file.
data_directory = os.path.join('.', 'data')

csv_file_path = os.path.join(data_directory, 'finefoods.csv')
df_file_path = os.path.join(data_directory, 'Reviews.csv')
text_file_path = os.path.join(data_directory, 'finefoods.txt')
gzip_file_path = text_file_path + '.gz'


# Function to download a file from a website.
#
# Assumes that the file is large and should be downloaded as a stream.
#
# url - URL to the file
# save_file_path - local path where the file should be saved
# chunk_size - size of the chunks to use when saving the streamed bytes to a file; default = 8KiB
#
# returns - None
def download_data_file(url, save_file_path, chunk_size=8192):
	if not os.path.exists(data_directory):
		os.mkdir(data_directory)

	# Request the file as a stream.
	response = requests.get(url, stream=True)

	# Save the data to the given file_path.
	with open(save_file_path, 'wb') as fd:
		for chunk in response.iter_content(chunk_size=chunk_size):
			fd.write(chunk)

	return None


# Function to decompress a gzipped data file.
#
# text_file - path to the decompressed file; will be created
# gzip_file - path to the gzipped file; must already exist
#
# returns - None
def decompress_data_file(text_file, gzip_file):
	with gzip.open(gzip_file, 'rb') as infile:
		with open(text_file, 'wb') as outfile:
			outfile.write(infile.read())

	return None


# Write a list of fields as a comma-separated row with a pipe (|) as a quote character.
def write_quoted_fields(csvfile, field_list):
	csvfile.write('|' + field_list[0] + '|')
	for field in field_list[1:]:
		csvfile.write(',|' + field + '|')
	csvfile.write("\n")


def convert_finefoods_data():
	print("convert_finefoods_data()")

	simple_header = [
		"productId",
		"userId",
		"profileName",
		"helpfulness",
		"score",
		"time",
		"summary",
		"text"]

	infile = open(text_file_path, "rt", encoding="Latin-1")
	csvfile = open(csv_file_path, "wt", encoding="UTF-8")


	# Write the header line.
	write_quoted_fields(csvfile, simple_header)

	# Useful controls during debugging.
	record_limit = 10000000

	field_count = len(simple_header)
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


# Adjust some of the columns.
def prepare_finefoods_data(infile_path, outfile_path):
	column_dtypes = {'productId': str, 'userId': str, 'profileName': str, 'helpfulness': str,
					 'score': np.float64, 'time': np.int64, 'summary': str, 'text': str}

	# For this dataset, 'quoting' must be set to QUOTE_ALL (1) and the quotechar to a pipe (|).
	# The problem is that values in some 'text' fields begin with a ", but don't end with one,
	# and many review texts contain commas, unbalanced quotes and apostrophes.
	review_df = pd.read_table(infile_path, delimiter=',', encoding="UTF-8", dtype=column_dtypes,
							  quoting=1, quotechar='|', engine="c", skip_blank_lines=True,
							  error_bad_lines=False, warn_bad_lines=True)

	# Convert score to an int, since it isn't truly a float.
	review_df['score'] = review_df['score'].astype(int)

	# Split helpfulness into 2 columns.
	review_df['helpfulness_numerator'] = [x[0] for x in review_df['helpfulness'].str.split('/')]
	review_df['helpfulness_denominator'] = [x[1] for x in review_df['helpfulness'].str.split('/')]

	# Convert the date into a datetime.
	review_df['date'] = pd.to_datetime(review_df['time'], unit='s')

	# Save the updated dataframe to a new file.
	review_df.to_csv(outfile_path, sep=',', encoding="UTF-8", quoting=1, quotechar='|')


# Downloads the compressed data file from the SNAP website.
class DownloadData(luigi.Task):
	def requires(self):
		return []

	def output(self):
		return luigi.LocalTarget(gzip_file_path)

	def run(self):
		print("DownloadData")
		print("Compressed file ('{}') does not exist".format(gzip_file_path))
		download_data_file(gzip_file_url, gzip_file_path)


# Decompresses the compressed data file.
class DecompressData(luigi.Task):
	def requires(self):
		return [DownloadData()]

	def output(self):
		return luigi.LocalTarget(text_file_path)

	def run(self):
		print("DecompressData")
		print("Decompressed file ('{}') does not exist".format(text_file_path))
		decompress_data_file(text_file_path, gzip_file_path)


# Converts the finefoods data from records with one field per line to a single CSV line (row).
class ConvertData(luigi.Task):
	def requires(self):
		return [DecompressData()]

	def output(self):
		return luigi.LocalTarget(csv_file_path)

	def run(self):
		print("ConvertData")
		print("CSV file ('{}') does not exist".format(csv_file_path))
		convert_finefoods_data()


# Prepares the data for analysis by splitting the helpfulness into a numerator and denominator.
class PrepareData(luigi.Task):
	def requires(self):
		return [ConvertData()]

	def output(self):
		return luigi.LocalTarget(df_file_path)

	def run(self):
		print("PrepareData")
		prepare_finefoods_data(csv_file_path, df_file_path)
		

class UploadDataToS3(luigi.Task):

    aws_access_key_id = "AKIAI7OXLP2L2QPSBU3Q"

    aws_secret_access_key = "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"

    def requires(self):
        return [PrepareData()]

    def input(self):
        return luigi.LocalTarget("C:/Users/Admin/Documents/DataScience/FinalProject/data/Reviews.csv")
    def run(self):
        conn=S3Connection(self.aws_access_key_id,self.aws_secret_access_key)
        bucket_name = 'team11'
        bucket = conn.get_bucket(bucket_name)
        k=Key(bucket)
        k.key = 'Reviews.csv' # to-do $$$$
        k.set_contents_from_string(self.input().path) # to-do $$$$
        print('uploading to S3')


if __name__ == '__main__':
	luigi.run()

