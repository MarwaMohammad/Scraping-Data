# Scraping-Data
Description:

GDELT is a large open dataset provided by Google. It basically consists of three parts: Events, Mentions and the Global Knowledge Graph (GKG). The full dataset is in the terabytes and updates every 15 minutes. For now, we just care about the Events dataset.

 

Google releases the data every 15 minutes as zipped CSV files which can be found here:

http://data.gdeltproject.org/gdeltv2/lastupdate.txt

 
This task is about:
Writing a Python script that checks if there is a new release of the Events dataset (the first link in the list). 
If there is a new dataset available, download the CSV file and save it as Parquet file.
The resulting Parquet file should grow incrementally, i.e., it should be updated whenever there is a new release – which is usually every 15 minutes. 
One important requirement is, that the resulting Parquet file does not contain duplicates.

 
