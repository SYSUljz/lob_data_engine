import click
import os
import sys

# Ensure local imports work
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import config
from src.etl import LOBPipeline
from src.training import LOBTrainer

@click.group()
def cli():
    """LOB Data Engine CLI - Professional Market Prediction Pipeline"""
    pass

@cli.command()
@click.option('--raw-path', default=config.RAW_DATA_PATH, help='Path to raw .parquet file')
@click.option('--output-dir', default=config.PROCESSED_DATA_DIR, help='Output directory for ml data')
@click.option('--groups', default=None, type=int, help='Number of row groups to process (None=All)')
@click.option('--batch-size', default=5, type=int, help='Batch size for chunked processing')
def etl(raw_path, output_dir, groups, batch_size):
    """Run the ETL Pipeline (Load -> Agg -> Feat -> Label -> Save)."""
    pipeline = LOBPipeline(raw_path, output_dir)
    pipeline.run(groups=groups, batch_size=batch_size)

@cli.command()
@click.option('--data-dir', default=config.PROCESSED_DATA_DIR, help='Directory containing train.parquet/val.parquet')
def train(data_dir):
    """Train One-vs-Rest Models detection all available clusters."""
    trainer = LOBTrainer(data_dir)
    trainer.run()

if __name__ == '__main__':
    cli()
