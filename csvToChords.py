import pandas
import argparse
import sys
import logging
import os
import datetime
import zoneinfo
import json
import pychords.tochords as tochords
import time


def stringToUnixTimestamp(datetimeString, year, tzinfo):
    """
    Convert a SunPower datetime string to a Unix timestamp.
    """

    # First try to parse as an iso string
    try:
        dt = datetime.datetime.fromisoformat(datetimeString)
        dt = dt.replace(tzinfo=tzinfo)
        return dt.timestamp()
    except Exception as e:
        pass

    # Next try to parse in Sunpower Datetime format (Saturday, 2/12/2022 - 7:00am - 8:00am for example)
    try:
        # Split out end and startdt times
        items = datetimeString.split("-")
        start = items[0] + items[1].strip()
        end = items[0] + items[2].strip()

        # Convert start/end to datetime (old files are missing year...)
        startdt = datetime.datetime.strptime(start, "%A, %m/%d/%Y %I:%M%p")
        enddt = datetime.datetime.strptime(end, "%A, %m/%d/%Y %I:%M%p")
        startdt = startdt.replace(tzinfo=tzinfo)
        enddt = enddt.replace(tzinfo=tzinfo)

        # If the end is before the start, assume the day has wrapped around
        if enddt < startdt:
            enddt = enddt + datetime.timedelta(days=1)

        # Find the middle of the two times
        dt = startdt + ((enddt - startdt) / 2)
        return dt.timestamp()
    except Exception as e:
        logging.error(f"Failed to parse {datetimeString}")
        raise e


def readDataFile(config, data_filepath, year=datetime.datetime.now().year, tzinfo=zoneinfo.ZoneInfo('US/Pacific')):
    """
    Read a csv or xlsx file into a pandas data frame and convert timestamps into unix timestamps.
    """

    # Make sure the file exists
    if os.path.exists(data_filepath) == False:
        logging.error(f"{data_filepath} does not exist.")
        return None

    # Parse the file
    logging.info(f"Parsing {data_filepath}")
    try:
        dataframe = pandas.read_excel(data_filepath)
    except ValueError as e:
        try:
            dataframe = pandas.read_csv(data_filepath, delimiter=config["delimiter"])
        except Exception as e:
            logging.error(f"Failed to parse {data_filepath} as CSV or XLSX file: {e}")
            return None

    # Convert time_column_name to a unix timestamp
    if config["time_column_name"] not in dataframe:
        logging.error(
            f"{data_filepath} does not contain a \"{config['time_column_name']}\" column, can not parse")
        return None
    dataframe["Unix Timestamp"] = dataframe[config["time_column_name"]].apply(
        stringToUnixTimestamp, args=(year, tzinfo,))

    return dataframe


def handleFile(config, file):
    """
    Handle a csv or xlsx file and send to chords.
    """

    # Parse the file into a dataframe
    df = readDataFile(config, file)
    if df is None:
        logging.error(f"Failed to parse {file}")
        sys.exit(-1)
    logging.debug("Data is:\n" + str(df))
    logging.debug("Headers are:\n" + str(list(df.columns)))

    # Create a list of column names and short names
    column_names = [x['column_name'] for x in config['variables']]
    short_names = [x['short_name'] for x in config['variables']]

    # Loop through all rows in the dataframe
    for i in df.index:
        df_row = df.loc[i]
        vars = {}

        # Make sure time exists
        if config["time_column_name"] not in df_row:
            logging.error(f"No time for row {i}")
            continue

        # Create the time stamp
        vars['at'] = df_row[config["time_column_name"]]

        # Collect the vars
        for (short_name, column_name) in zip(short_names, column_names):
            if column_name not in df_row:
                logging.debug(f"Skipping unrecognized column {short_name}, {column_name}")
                continue
            if pandas.notna(df_row[column_name]):
                vars[short_name] = df_row[column_name]

        # Send to CHORDS
        sendData(config=config, vars=vars)


def sendData(config: dict, vars: dict) -> None:
    """
    Send one data record to chords.

    vars contains a dictionary of variables, referenced by short name. There must 
    be an 'at' element containing the linux timestamp.
    """

    # Build and send the URI
    chords_record = {}
    chords_record["inst_id"] = config["instrument_id"]
    chords_record["api_email"] = config["api_email"]
    chords_record["api_key"] = config["api_key"]
    chords_record["vars"] = vars
    uri = tochords.buildURI(config["chords_host"], chords_record)
    logging.info(f"Submitting: {uri}")
    max_queue_length = config.get("max_queue_length", 31*60*24)
    if not config["test"]:
        tochords.submitURI(uri, max_queue_length)
    time.sleep(config["sleep_secs"])


def main(files: list, config_file: str, test_mode: bool):

    # Load configuration
    logging.info(f"Starting CsvToChords to Chords with {config_file}")
    config = json.loads(open(config_file).read())
    config["test"] = test_mode

    # Startup chords sender
    if not config["test"]:
        tochords.startSender()

    # Parse each csv file
    for file in files:
        logging.info(f"Handling: {file}")
        handleFile(config, file)

    # Wait for all data to be sent
    while True:
        num_remaining = tochords.waiting()
        logging.info(f"Queue length: {num_remaining}")
        time.sleep(1)
        if num_remaining == 0:
            break


if __name__ == '__main__':

    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-f', '--files', help='The files to compare.', required=True, nargs='+')
    parser.add_argument(
        "-c", "--config", help="Path to json configuration file to use.", required=True)
    parser.add_argument(
        "-t", "--test", help="Run in test mode", action="store_true", default=False)
    parser.add_argument(
        "--debug", help="Enable debug logging",
        action="store_true")
    args = parser.parse_args()

    # Configure logging
    level = logging.INFO
    if args.debug:
        level = logging.DEBUG
    logging.basicConfig(stream=sys.stdout, level=level, format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logging.debug("Debug logging enabled")

    # Run main
    main(args.files, args.config, args.test)
