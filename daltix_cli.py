# -*- coding: utf-8 -*-
"""
Created on Wed Jun 30 01:19:49 2021

@author: Prashant Tyagi
"""
import click
from base_logger import logger
from transform import transform
from support import upload_file_to_other_cloud_providers, \
    download_file_from_other_cloud_providers
from aws_s3_helper import upload_file_to_s3, download_file_from_s3
from web_hook_request import web_hook_post_message


@click.command()
@click.argument('input-location')
@click.option('--input-location-type', help="type of the input: local(default)/aws s3/or any other input")
@click.option('--output-location', help="location of the output folder only in case of aws s3 or other cloud provider")
@click.option('--output-location-type', help="type of the output: local/aws s3/or any other input")
@click.option('--webhook-url', help="webhook URL for writing POST requests")
@click.option('--report-format', help="report format types: csv, tsv, pdf")
def convert(input_location, input_location_type, output_location,
            output_location_type, webhook_url, report_format):
    """
    Daltix Data Inspector(ddi) CLI

    It takes input one or more gzipped JSON lines files and produces as
    output as a report about the contents of the data.
    :param report_format: str
    :param webhook_url: str
    :param output_location_type: str
    :param output_location: str
    :param input_location_type: str
    :param input_location: str
    """
    print(f"Input Location: {input_location}")
    print(f"Input Location Type: {input_location_type}")
    print(f"Output Location: {output_location}")
    print(f"Output Location Type: {output_location_type}")
    print(f"Webhook URL: {webhook_url}")
    print(f"Report Format: {report_format}")

    logger.info("Execution Started")
    # pipeline for file as aws input
    if input_location_type == "aws s3":
        # find bucket name from the input_location
        bucket = input_location.split('//')[1].split('/')[0]
        prefix = ""
        if len(input_location.split('//')[1].split('/')) > 2:
            prefix = input_location.split('//')[1].split('/')[1] + "/" + input_location.split('//')[1].split('/')[2]

        input_directory = download_file_from_s3(prefix, ".", bucket, logger)
        # transform method will create the report in the output directory.
        # The name of the report will be timestamped
        file_name = transform(input_directory, report_format, logger)
        if file_name:
            logger.info(f'Report: {file_name} generated successfully')

        if output_location_type == "aws s3":
            bucket = output_location.split('//')[1].split('/')[0]
            if upload_file_to_s3(file_name, bucket, logger):
                logger.info("Pipeline from AWS S3 to AWS S3 executed successfully")

        elif output_location_type not in ["local", "aws s3"]:
            logger.warning(f"Pipeline from AWS to {output_location_type} requires support")
            upload_file_to_other_cloud_providers(file_name, output_location_type, logger)
            web_hook_post_message({"text": "Failure"}, webhook_url, logger)

        web_hook_post_message({"text": "Success"}, webhook_url, logger)
        logger.info("Pipeline from AWS S3 to local executed successfully")

    # pipeline for file as local input
    elif input_location_type == "local":
        file_name = transform(input_location, report_format, logger)
        if file_name:
            logger.info(f'Report: {file_name} generated successfully')
        if output_location_type == "aws s3":
            bucket = output_location.split('//')[1].split('/')[0]
            if upload_file_to_s3(file_name, bucket, logger):
                logger.info("Pipeline from AWS S3 to AWS S3 executed successfully")

        elif output_location_type not in ["local", "aws s3"]:
            logger.warning(f"Pipeline from local to {output_location_type} requires support")
            upload_file_to_other_cloud_providers(file_name, output_location_type, logger)
            logger.error("Can't post the failure message")
            web_hook_post_message({"text": "Failure"}, webhook_url, logger)

        logger.info("Pipeline from local to local executed successfully")
        web_hook_post_message({"text": "Success"}, webhook_url, logger)

    # pipeline for files as neither aws nor local input
    elif input_location_type not in ["local", "aws s3"]:
        # Need to add further support
        logger.warning(f"Pipeline from {input_location_type} to {output_location_type} requires support")
        web_hook_post_message({"text": "Failure"}, webhook_url, logger)


if __name__ == '__main__':
    convert(prog_name='convert')
