
import argparse

from experiments import DeltaExperiment, HudiExperiment, IcebergExperiment


def run_experiments(table_format: str, operation: str):
    experiments = {
        'iceberg': IcebergExperiment,
        'delta': DeltaExperiment,
        'hudi': HudiExperiment,
    }
    experiments[table_format]().run(operation=operation)

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
        "--operation",
        required=True,
        help="Run a specific operation of the experiments."
    )
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_command_line_arguments()
    run_experiments(table_format=args.table_format, operation=args.operation)