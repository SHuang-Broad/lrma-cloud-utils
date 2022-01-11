import logging
import logging.handlers
import os
import sys
from typing import List

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, From, To, Subject, PlainTextContent, HtmlContent


def get_configured_logger(log_level: int = logging.DEBUG,
                          flush_level: int = logging.ERROR,
                          buffer_capacity: int = 0,
                          formatter: logging.Formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')):

    handler = __config_stream_or_file_handler(log_level, formatter)
    memory_handler = __config_memory_handler(buffer_capacity, flush_level, handler)

    logger = logging.getLogger()
    logger.setLevel(log_level)
    logger.addHandler(memory_handler)
    return logger


def __config_stream_or_file_handler(log_level: int, formatter: logging.Formatter, log_file: str = None):
    if log_file:
        handler = logging.FileHandler(log_file)
    else:
        handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(log_level)
    handler.setFormatter(formatter)
    return handler


def __config_memory_handler(capacity: int, flush_level: int, handler: logging.handlers):
    return logging.handlers.MemoryHandler(
        capacity=capacity,
        flushLevel=flush_level,
        target=handler
    )


def get_dict_depth(d: dict, level: int) -> int:
    """
    Simple utility (using recursion) to get the max depth of a dictionary
    :param d: dict to explore
    :param level: current level
    :return: max depth of the given dict
    """
    cls = [level]
    for _, v in d.items():
        if isinstance(v, dict):
            cls.append(get_dict_depth(v, 1 + level))

    return max(cls)


def is_contiguous(arr: list) -> bool:
    arr_shift = arr[1:]
    arr_shift.append(arr[-1]+1)
    return all(1 == (a_i - b_i) for a_i, b_i in zip(arr_shift, arr))


def absolute_file_paths(directory):
    """https://stackoverflow.com/a/9816863"""
    for dirpath, _ ,filenames in os.walk(directory):
        for f in filenames:
            yield os.path.abspath(os.path.join(dirpath, f))


def send_notification(logger: logging.Logger,
                      notification_sender_name: str,
                      notification_receiver_names: List[str], notification_receiver_emails: List[str],
                      email_subject: str, email_body: str,
                      html_body: str = None):
    """
    Send a Single Email to a Single Recipient
    Shameless copy from
    https://github.com/sendgrid/sendgrid-python/blob/main/examples/helpers/mail_example.py#L9

    Provide html_body at your own risk.
    :return:
    """

    assert "SENDGRID_API_KEY" in os.environ, \
        'environment variable SENDGRID_API_KEY is needed.'
    assert "SENDER_EMAIL" in os.environ, \
        'environment variable SENDER_EMAIL is needed.'

    if len(notification_receiver_emails) != len(notification_receiver_names):
        raise ValueError("Different number of recipients and recipients' emails")

    sg = SendGridAPIClient(api_key=os.environ.get('SENDGRID_API_KEY'))
    notification_sender_email = os.environ.get('SENDER_EMAIL')
    failed_responses = list()
    for i in range(len(notification_receiver_emails)):
        message = Mail(from_email=From(notification_sender_email, notification_sender_name),
                       to_emails=To(notification_receiver_emails[i], notification_receiver_names[i]),
                       subject=Subject(email_subject),
                       plain_text_content=PlainTextContent(email_body),
                       html_content=HtmlContent(html_body) if html_body else None)
        response = sg.client.mail.send.post(request_body=message.get())
        if 202 != response.status_code:
            failed_responses.append(i)
    if 0 < len(failed_responses):
        failures = '\n'.join([notification_receiver_names[i]+':'+notification_receiver_emails[i]
                              for i in failed_responses])
        logger.warning(f"Failed to send message to some receivers: {failures}")

