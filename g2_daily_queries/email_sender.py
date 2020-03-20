#!/usr/bin/env python3

import json
import configparser
import datetime
import os.path
import subprocess
from argparse import ArgumentParser
from email.mime.text import MIMEText
from smtplib import SMTP
from email.mime.multipart import MIMEMultipart

SENDER = "big4@jsonar.com"


class EmailSender():
    """Send email summarizing the amount of pipelines that have been run in
    the past 24 hours, how many of them failed and how many succeeded.
    """

    def __init__(self, config_file="~/.smtp/credentials"):
        """Initialize connection to SonarW and login to gmail."""
        self.credentials = os.path.expanduser(config_file)
        self.config = configparser.ConfigParser()
        self.config.read(self.credentials)
        self.server = SMTP(self.config.get("configuration", "smtp"))
        self.server.starttls()
        self.server.login(self.config.get("configuration", "smtp_username"),
                          self.config.get("configuration", "smtp_password"))

    def get_message_string(self, file, type):
        """Return string containing info retrieved from
        'get_string_from_counter_dict' for regular pipelines, group pipelines
        and queries.
        """
        with open(file) as f:
            string = "{:1}".format(type + ":\n")
            x = 0
            for row in json.load(f):
                x += 1
                string += ('#' + str(x) + ': ' + str(row) + '\n')

        return string


def create_message(recipients, msg_content, msg_subject):
    """Create message to be emailed and format it."""
    msg = MIMEText(msg_content)
    msg['Subject'] = msg_subject
    msg['From'] = SENDER
    msg['To'] = ", ".join(recipients)
    return msg


def parse():
    parser = ArgumentParser()
    parser.add_argument('--inserts',
                        help='Absolut path to send the file from')
    parser.add_argument('--updates',
                        help='Absolut path to send the file from')
    parser.add_argument('--queries',
                        help='Absolut path to send the file from')
    parser.add_argument('--recipients', nargs='+',
                        help='List of recipients to send emails to, separated'
                             ' by spaces. Example:'
                             + '--recipients fender@gibson.com god@heaven.com '
                               'harley@davidson.com')
    parser.add_argument('--subject',
                        help='Subject of the email.')
    return vars(parser.parse_args())


def main():
    """Initialize instance of 'EmailSender' and use it to send email."""
    print(str(datetime.datetime.now()) + " - Starting to pull sonarw log file.")
    args = parse()
    sender = EmailSender()
    insert = sender.get_message_string(args['inserts'], "Inserts")
    update = sender.get_message_string(args['updates'], "Updates")
    query = sender.get_message_string(args['queries'], "Queries")
    msg_content = insert + "\n" + update + "\n" + "\n" + query

    message = create_message(args['recipients'], msg_content, args['subject'])
    sender.server.sendmail(SENDER, args['recipients'], message.as_string())
    print(str(datetime.datetime.now()) + " - Email sent.")
    print(str(datetime.datetime.now()) + " - Finished generating report.")
    sender.server.quit()


if __name__ == "__main__":
    main()
