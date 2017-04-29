import luigi
import time
from zipfile import ZipFile
import urllib
from tempfile import mktemp
import os
import numpy as np
import pandas as pd
from IPython.display import display
import requests
import gzip
class DownloadFoodReviewsData(luigi.Task):
	def run(self):
		# URL for the file download.
		gzip_file_url = "https://snap.stanford.edu/data/finefoods.txt.gz"

		# Paths for the data directory, text file, and compressed file.
		data_directory = os.path.join('..', 'data')
		if not os.path.exists(data_directory):
			print("Data directory ('{}') does not exist - creating it".format(data_directory))
			os.mkdir(data_directory)

		text_file_path = os.path.join(data_directory, 'finefoods.txt')
		gzip_file_path = text_file_path + '.gz'
		response = requests.get(gzip_file_url, stream=True)
		chunk_size=8192
		# Save the data to the given file_path.
		with open(save_file_path, 'wb') as fd:
			for chunk in response.iter_content(chunk_size=chunk_size):
				fd.write(chunk)
		if os.path.exists(text_file):
			print("Decompressed file ('{}') already exists".format(text_file))
		else:
			print("Decompressed file ('{}') does not exist".format(text_file))
				
		# To get the decompressed file, we need the gzipped file.
		if not os.path.exists(gzip_file):
			print("Compressed data file ('{}') does not exist, downloading...".format(gzip_file))
			download_data_file(gzip_file_url, gzip_file)
			print("...download finished")

			print("Compressed data file ('{}') exists, decompressing".format(gzip_file))
					
		
	def output(self):	
		#save file to Data directory
		return luigi.LocalTarget('data/finefoods.txt')	
		
		
# if __name__ == "__main__":

#     luigi.run()


