# -*- coding: utf-8 -*-
"""
Created on Tue Jun 29 14:20:38 2021

@author: Prashant Tyagi
"""
import pandas as pd
import os
import glob
from datetime import datetime
import csv


def transform(input_address, file_type, logger):
    """
    This function takes the following parameters and generates a report over
    them

    Parameters
    ----------
    input_address : string
        local address to the directory containing input gzip files.
    file_type : string
        format of the report (csv or tsv). Only these two are allowed at the
        moment

    Returns
    -------
    file_path : string
        report absolute address.

    """
    date = datetime.now().strftime("%Y%m%d%I%M%S")
    file_path = ""

    if file_type == "tsv" or file_type == "csv":
        input_address = os.path.abspath(input_address)
        output_address = os.path.abspath("output/")
        print(input_address)
        print(output_address)

        if not os.path.isdir(output_address):
            logger.info("Created directory")
            os.mkdir(output_address)

        # Report name with timestamp
        report_name = "report_" + date + "." + file_type
        file_path = os.path.join(output_address, report_name)

        try:
            with open(file_path, 'w+', newline='') as out_file:
                tsv_writer = csv.writer(out_file, delimiter='\t' if file_type == "tsv" else ',')
                tsv_writer.writerow(['shop', 'country', 'location',
                                     'resource_type', 'resource_count',
                                     'has_name', 'has_name_pct',
                                     'has_brand', 'has_brand_pct',
                                     'has_description', 'has_description_pct',
                                     'has_images', 'has_images_pct',
                                     'has_price', 'has_price_pct',
                                     'has_promo', 'has_promo_pct'])

                logger.info(f"{report_name} column metadata")
                out_file.close()


        except:
            logger.error("Some issue creating File")

        for file in glob.glob(input_address + "\*.gz"):
            logger.info(f"Working on {file}. This will be sent to pandas pipeline to generate reports")
            file = os.path.abspath(file)
            # Opening gzip as a pandas dataframe
            df = pd.read_json(file, lines=True, compression='gzip')

            # Splitting resource into resource_uri and resource_type columns
            resource = pd.json_normalize(df['resource'])
            resource.rename(columns={'uri': 'resource_uri'}, inplace=True)

            # Manipulating the dataframe a little bit
            df.drop(columns=['resource'], inplace=True)
            df['resource_uri'] = resource['resource_uri']
            df['resource_type'] = resource['resource_type']
            df['pricing'] = df['pricing'].astype(str)

            # Grouping and splitting data
            report = df.groupby(['shop', 'country', 'location'])

            # Writing data to tsv or csv in append mode
            with open(file_path, mode='a', newline='') as out_file:
                tsv_writer = csv.writer(out_file, delimiter='\t' if file_type == "tsv" else ',')
                for name, group in report:
                    shop, country, location = name

                    # Aggregate data calculation using pandas
                    has_name_pct = round(group['name'].count().sum() / len(group['name']), 2)
                    has_name = True if has_name_pct > 0 else False
                    has_brand_pct = round(group['brand'].count().sum() / len(group['brand']), 2)
                    has_brand = True if has_brand_pct > 0 else False
                    has_description_pct = round(group['description'].count().sum() / len(group['description']), 2)
                    has_description = True if has_description_pct > 0 else False
                    has_images_pct = round(group['images'].count().sum() / len(group['images']), 2)
                    has_images = True if has_images_pct > 0 else False
                    has_price_pct = round(group['pricing'].str.contains('prices').sum() / len(group['pricing']), 2)
                    has_price = True if has_images_pct > 0 else False
                    has_promo_pct = round(group['pricing'].str.contains
                                          ('promo_strings').sum() / len(group['pricing']), 2)
                    has_promo = True if has_promo_pct > 0 else False
                    resource_type = group['resource_type'].value_counts().index.tolist()
                    resource_type = ','.join(resource_type)
                    resource_count = group['resource_type'].count().sum()

                    # Writing computed values to the report
                    tsv_writer.writerow([shop, country, location,
                                         resource_type, resource_count,
                                         has_name, has_name_pct,
                                         has_brand, has_brand_pct,
                                         has_description, has_description_pct,
                                         has_images, has_images_pct,
                                         has_price, has_price_pct,
                                         has_promo, has_promo_pct])

            logger.info("Summarized data written to the report")
            logger.info(f'Dataframe Information: {df.info()}')
            # Clearing dataframe, it improved performance
            df = ''
            out_file.close()

    return file_path

# input_address = os.path.abspath("./software_engineer_challenge")
# transform(input_address, "output/", "csv")
