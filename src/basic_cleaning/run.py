#!/usr/bin/env python
"""
 Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import pandas as pd
import wandb


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")


def go(args):

    logging.info('Starting basic_cleaning')

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    input_artifact = run.use_artifact(args.input_artifact).file()

    df = pd.read_csv(input_artifact)

    df = df[df['price'].between(args.min_price, args.max_price)]
    logging.info('Filtered dataframe to contain prices between %f and %f', args.min_price, args.max_price)

    # Convert last_review to date time type
    df['last_review'] = pd.to_datetime(df['last_review'] )
    logging.info('Converted last review to date time')

    # FIlter out rows with longitude and latitudes outside of the expected value
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    try:
        df.to_csv(args.output_artifact, index=False)
        logging.info('Exported clean dataframe to file location %s', args.output_artifact)
    except IOError as err:
        logging.info('Unable to export dataframe to file location %s. %s', args.output_artifact, err)

    try:
        artifact = wandb.Artifact(
            args.output_artifact,
            type=args.output_type,
            description=args.output_description,
        )
        artifact.add_file("clean_sample.csv")
        run.log_artifact(artifact)
        logging.info('Logged exported clean dataframe as artifact.')
    except Exception as err:
        logging.info('Unable to log exported clean dataframe as artifact. %s' % err)

    run.finish()

    logging.info('Completed basic_cleaning')

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description=" Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact")


    parser.add_argument(
        "--input_artifact",
        type=str,
        help="Input artifact to apply basic cleaning to.",
        required=True
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="Output artifact generated post basic cleaning",
        required=True
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help='',
        required=True
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help='Export of input dataframe with basic cleaning applied',
        required=True
    )

    parser.add_argument(
        "--min_price",
        type= float,
        help= "Minimum price to include post cleaning",
        required=True
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help= "Maximum price to include post cleaning",
        required=True
    )


    args = parser.parse_args()

    go(args)
