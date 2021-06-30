# -*- coding: utf-8 -*-
"""
Created on Tue Jun 29 14:20:04 2021

@author: Prashant Tyagi
"""

import boto3
import os

s3_client = boto3.client('s3')


# Downloading method
def download_file_from_s3(prefix, local, bucket, logger, client=s3_client):
    """
    Parameters
    ----------
    - prefix: pattern to match in s3
    - local: local path to folder in which to place files
    - bucket: s3 bucket with target contents
    - client: initialized s3 client object
    - logger: for logging
    
    Returns
    -------
    string
        location to the download files
    """
    keys = []
    dirs = []
    next_token = ''
    base_kwargs = {
        'Bucket': bucket,
        'Prefix': prefix,
    }
    downloaded_directory = []
    while next_token is not None:
        kwargs = base_kwargs.copy()
        if next_token != '':
            kwargs.update({'ContinuationToken': next_token})
        results = client.list_objects_v2(**kwargs)
        contents = results.get('Contents')
        for i in contents:
            k = i.get('Key')
            if k[-1] != '/':
                keys.append(k)
            else:
                dirs.append(k)
        next_token = results.get('NextContinuationToken')
    for d in dirs:
        dest_pathname = os.path.join(local, d)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
    for k in keys:
        dest_pathname = os.path.join(local, k)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
        client.download_file(bucket, k, dest_pathname)

    return keys[0].split('/')[0]


def upload_file_to_s3(file_name, bucket_name, logger, object_name=None):
    """
    Parameters
    ----------
    file_name : string
        Name of the file for upload.
    bucket_name : string
        Name of the bucket to which file will be uploaded.
    logger: object
        for logging
    object_name : Object, optional
        DESCRIPTION. The default is None.

    Returns
    -------
    bool
        Success or not

    """
    if object_name is None:
        object_name = file_name

    # Upload the file
    try:
        response = s3_client.upload_file(file_name, bucket_name, object_name)
        if response:
            logger.info(f"{file_name} Sent to the {bucket_name}")
    except Exception as errors:
        logger.error(errors)
        logger.error("Please configure your credentials for the bucket you want to push your data to")
        return False

    return True
