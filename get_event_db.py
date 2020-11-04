#!/usr/bin/env python
# coding: utf-8
import gc
import multiprocessing
import pandas as pd
import zipfile
import requests as req
import re
import time
from time import time, sleep

# Find_url is used to return the all url in the response
def Find_url(string):
    # findall() has been used
    # with valid conditions for urls in string
    regex = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"
    url = re.findall(regex,string)
    return [x[0] for x in url]

# find_event_doc_id is used to get the id of the document
def find_event_doc_id(content):
    doc_ids = [(word) for word in content.split() if word.isdigit()]
    return doc_ids[0]

# get_zip is used to get the zipped file from the url
def get_zip(event_url,event_doc_id):
    resp_zip = req.get(event_url)
    zip_file_name = 'event_id'+(event_doc_id)+'.zip'
    zip_file = open(zip_file_name, 'wb')
    zip_file.write(resp_zip.content)
    zip_file.close()
    return zip_file_name


# read_zip_into_dataframe is used to create the datframe from zip file
def read_zip_into_dataframe(zip_file_name):
    columns_names = ['GlobalEventID', 'SQLDATE', 'MonthYear', 'Year', 'FractionDate', 'Actor1Code', 'Actor1Name', 'Actor1CountryCode',
                     'Actor1KnownGroupCode', 'Actor1EthnicCode', 'Actor1Religion1Code', 'Actor1Religion2Code', 'Actor1Type1Code',
                     'Actor1Type2Code', 'Actor1Type3Code', 'Actor2Code', 'Actor2Name', 'Actor2CountryCode', 'Actor2KnownGroupCode', 'Actor2EthnicCode',
                     'Actor2Religion1Code', 'Actor2Religion2Code', 'Actor2Type1Code', 'Actor2Type2Code', 'Actor2Type3Code', 'IsRootEvent', 'EventCode',
                     'EventBaseCode', 'EventRootCode', 'QuadClass', 'GoldsteinScale', 'NumMentions', 'NumSources', 'NumArticles', 'AvgTone', 'Actor1Geo_Type',
                     'Actor1Geo_Fullname', 'Actor1Geo_CountryCode', 'Actor1Geo_ADM1Code', 'Actor1_private_code', 'Actor1Geo_Lat', 'Actor1Geo_Long',
                     'Actor1Geo_FeatureID', 'Actor2Geo_Type', 'Actor2Geo_FullName', 'Actor2Geo_CountryCode', 'Actor2Geo_ADM1Code', 'Actor2_private_code',
                     'Actor2Geo_Lat', 'Actor2Geo_Long', 'Actor2Geo_FeatureID', 'ActionGeo_Type', 'ActionGeo_FullName', 'ActionGeo_CountryCode',
                     'ActionGeo_ADM1Code', 'Action_private_code', 'ActionGeo_Lat', 'ActionGeo_Long', 'ActionGeo_FeatureID', 'DATEADDED', 'SOURCEURL']
    # I used ta since the website https://www.gdeltproject.org/data.html#rawdatafiles mentioned that fies are tabed delimited.
    #zip_file_name_path = 'D:\\ML_courses\\DataAnalysis\\Professional\\Internship\\'+ zip_file_name
    df = pd.read_csv(zip_file_name, compression ='zip', sep='\t', names = columns_names)
    return df


#This unction is used to check if there is new udates in the content of the response, thern there is new file has been uploaded.
def check_new_udate(page_url,f_content):
    resp = req.get(page_url)
    content=''
    if resp.text == f_content:
        print('No updates')
    else:
        print("updated")
        content = resp.text
    return content


#This function is used
def read_dataframe(content):
    #To get the required url
    event_url = Find_url(content)[0]

    #To get the event file Id
    event_doc_id = find_event_doc_id(content)

    #To get the csv file name
    file_name_index = event_url.find('v2/')+3
    csv_file_name = event_url[file_name_index:-4]

    zip_file_name = get_zip(event_url,event_doc_id)

    df = read_zip_into_dataframe(zip_file_name)
    return df, csv_file_name


def main_Process():
    start_time = time()
    page_url = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
    resp = req.get(page_url)
    f_content = resp.text
    df, csv_file_name = read_dataframe(f_content)

    df.to_csv(csv_file_name, index=False)
    df.to_parquet('event.parquet', engine='fastparquet',  compression='gzip')
    end_time = time()
    sleep((60*15)-(end_time-start_time))

    print("1st 15 minutes has gone")
    while True:
        start_time = time()
        #To check if there is any update o the website.
        content = check_new_udate(page_url, f_content)
        if content != "":
            # Create new dataframe with new event file data
            new_df, new_csv_file_name = read_dataframe(content)
            # Create new cvs data every 15 Minutes
            new_df.to_csv(new_csv_file_name, index=False)
            # combine the new dataframe to the previous dataframe
            combined_df = df.append(new_df, ignore_index=True)
            # Drop duplicates if any after outjoin
            mask = combined_df.duplicated()
            duplicated_df = combined_df.loc[mask]
            if duplicated_df.empty:
                print("No duplicates found")
            else:
                print("There is duplicates")
                combined_df.drop_duplicates(inplace=True)
            # save the combined data frame to the parquet file.
            combined_df.to_parquet('event.parquet', engine='fastparquet',  compression='gzip', index = False)
            end_time = time()
            # To sleep time equal to 15 - processing time
            sleep((60*15)-(end_time-start_time))
            print("The new 15 minutes has gone")
            # Save the previous data frames to df to be combined to the new dataframe in the next iteration.
            df = combined_df
            # Save the old content to be compared to the new content.
            f_content = content
        else:
            sleep(60*10)

p1 = multiprocessing.Process(target=main_Process, args=())


def main():
    p1.start()
    quit = input()
    if quit.lower() == 'q':
        p1.terminate()
        gc.collect()




if __name__ == "__main__":
    print('\nWould you like to quit? Please, Enter q.\n')
    main()
