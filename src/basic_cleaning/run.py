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

    run = wandb.init(project="nyc_airbnb", job_type="basic_cleaning")
    run.config.update(args)

    logger.info(f"input_artifact paramater is {args.input_artifact}")
    logger.info(f"output_artifact paramater is {args.output_artifact}")
    logger.info(f"output_type paramater is {args.output_type}")
    logger.info(f"output_description paramater is {args.output_description}")
    logger.info(f"min_price paramater is {args.min_price}")
    logger.info(f"max_price paramater is {args.max_price}")

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    local_path = wandb.use_artifact(args.input_artifact).file()

    # load file
    df = pd.read_csv(local_path)

    # Drop outliers
    min_price = args.min_price
    max_price = args.max_price
    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()
    # Convert last_review to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Save dataframe to clean_sample.csv 
    df.to_csv("clean_sample.csv", index=False)

    # upload clean_sample artifact to wandb
    artifact = wandb.Artifact(
     args.output_artifact,
     type=args.output_type,
     description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type = str,
        help = "The location of the input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type = str,
        help = "The location of the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type = str,
        help = "The type of the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type = str,
        help = "The description of the output description",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type = float,
        help = "The minimum price",
        required = True
    )

    parser.add_argument(
        "--max_price", 
        type = float,
        help = "The maximum price",
        required=True
    )


    args = parser.parse_args()

    go(args)
