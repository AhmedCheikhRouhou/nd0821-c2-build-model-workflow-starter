#!/usr/bin/env python
"""
 Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    logger.info("Download input artifact")
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(artifact_local_path)
    logger.info("Cleaning Data")
    # Drop outliers
    min_price = args.min_price
    max_price = args.max_price
    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()
    # Convert last_review to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])
    
    
    logger.info("Uploading to W&B")
    artifact = wandb.Artifact(
     args.output_artifact,
     type=args.output_type,
     description=args.output_description,
    )
    

    idx = df['longitude'].between(-74.15, -73.60) & df['latitude'].between(40.6, 41.1)
    df = df[idx].copy()

    df.to_csv("clean_sample.csv", index=False)
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="input csv file",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="output csv file",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Job Type",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Min outlier for price",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="max outlier for price",
        required=True
    )


    args = parser.parse_args()

    go(args)
