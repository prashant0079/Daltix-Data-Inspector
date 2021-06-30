# -*- coding: utf-8 -*-
"""
Created on Wed Jun 30 01:19:49 2021

@author: Prashant Tyagi
"""
import json
import requests

"""
    This function takes the following parameters and generates a report over
    them

    Parameters
    ----------
    json_message : JSON Object
        Kind of message you want to post over Webhook URL
    webhook_url : string
        format of the report (csv or tsv). Only these two are allowed at the
        moment
    """


def web_hook_post_message(json_message, webhook_url, logger):
    if webhook_url is not None and webhook_url != "":
        response = requests.post(
            webhook_url, data=json.dumps(json_message),
            headers={'Content-Type': 'application/json'}
        )
        if response.status_code != 200:
            print("Failure in executing POST request")
            logger.error("Some issue while executing the POST request")
            raise ValueError(
                'Request to webhook returned an error %s, the response is:\n%s'
                % (response.status_code, response.text)
            )
        print("Success")
        logger.info("Message successfully posted over the webhook url")

    # If the webhook is mandatory, please uncomment the following code
    # else:
    #     logger.warning("Please enter webhook url")
    #     print("Please enter webhook url")
