
import argparse

from experiments import DeltaExperiment, HudiExperiment, IcebergExperiment


def run_experiments(table_format: str, operation: str, s3_path: str, experiment_id: str, scale_in_gb: int):
    experiments = {
        'iceberg': IcebergExperiment,
        'delta': DeltaExperiment,
        'hudi': HudiExperiment,
    }
    experiments[table_format](
        scale_in_gb=scale_in_gb, 
        path=s3_path,
        experiment_id=experiment_id
    ).run(operation=operation)

def parse_command_line_arguments():
    parser = argparse.ArgumentParser(
        description="Data Lake Table Formats Performance Experiment",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(        
        "--table-format",
        required=True,
        help="Run the experiments using a specific table format."
    )
    parser.add_argument(        
        "--scale-in-gb",
        required=True,
        default=1,
        help="Run the experiments using a specific table format."
    )
    parser.add_argument(        
        "--operation",
        required=True,
        help="Run a specific operation of the experiments."
    )
    parser.add_argument(        
        "--s3-path",
        required=True,
        help="Define the S3 path to store data."
    )
    parser.add_argument(        
        "--experiment-id",
        required=False,
        help="Define the S3 path to store data.",
        default=None
    )
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_command_line_arguments()
    run_experiments(table_format=args.table_format, operation=args.operation, s3_path=args.s3_path, experiment_id=args.experiment_id, scale_in_gb=args.scale_in_gb)