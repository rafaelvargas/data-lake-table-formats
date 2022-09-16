
import os
import shlex
import subprocess
import argparse
import time
from datetime import datetime

SCALA_VERSION = "2.12"
ICEBERG_VERSION = ""
DELTA_VERSION = "2.0.0"
HUDI_VERSION = "0.11.1"
PACKAGES = {
    "delta": f"io.delta:delta-core_{SCALA_VERSION}:{DELTA_VERSION},io.delta:delta-contribs_{SCALA_VERSION}:{DELTA_VERSION},io.delta:delta-hive_{SCALA_VERSION}:0.2.0",
    "iceberg": f"org.apache.iceberg:iceberg-spark-runtime-3.2_{SCALA_VERSION}:{ICEBERG_VERSION}",
    "hudi": f"org.apache.hudi:hudi-spark3.2-bundle_{SCALA_VERSION}:{HUDI_VERSION}"
}

def run_cmd(cmd, throw_on_error=True, env=None, stream_output=False, **kwargs):
    if isinstance(cmd, str):
        cmd = shlex.split(cmd)
    cmd_env = os.environ.copy()
    if env:
        cmd_env.update(env)

    if stream_output:
        child = subprocess.Popen(cmd, env=cmd_env, **kwargs)
        exit_code = child.wait()
        if throw_on_error and exit_code != 0:
            raise Exception("Non-zero exitcode: %s" % (exit_code))
        return exit_code
    else:
        child = subprocess.Popen(
            cmd,
            env=cmd_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **kwargs)
        (stdout, stderr) = child.communicate()
        exit_code = child.wait()
        if throw_on_error and exit_code != 0:
            raise Exception(
                "Non-zero exitcode: %s\n\nSTDOUT:\n%s\n\nSTDERR:%s" %
                (exit_code, stdout, stderr))
        return exit_code, stdout, stderr


def run_cmd_over_ssh(cmd, host, ssh_id_file, user, **kwargs):
    full_cmd = f"""ssh -i {ssh_id_file} {user}@{host} "{cmd}" """
    print(full_cmd)
    return run_cmd(full_cmd, **kwargs)

# def wait_for_completion(cluster_hostname, ssh_id_file, benchmark_id, ssh_user, copy_report=True):
#     completed = False
#     succeeded = False

#     print(f"\nWaiting for completion of experiment id {benchmark_id}")
#     while not completed:
#         # Print the size of the output file to show progress
#         (_, out, _) = run_cmd_over_ssh(f"stat -c '%n:   [%y]   [%s bytes]' {output_file}",
#                                         cluster_hostname, ssh_id_file, ssh_user,
#                                         throw_on_error=False)
#         out = out.decode("utf-8").strip()
#         print(out)
#         if "No such file" in out:
#             print(">>> Benchmark failed to start")
#             return

#         # Check for the existence of the completed file
#         (_, out, _) = run_cmd_over_ssh(f"ls {completed_file}", cluster_hostname, ssh_id_file, ssh_user,
#                                         throw_on_error=False)
#         if completed_file in out.decode("utf-8"):
#             completed = True
#         else:
#             time.sleep(60)

#     # Check the last few lines of output files to identify success
#     (_, out, _) = run_cmd_over_ssh(f"tail {output_file}", cluster_hostname, ssh_id_file, ssh_user,
#                                     throw_on_error=False)
#     if "SUCCESS" in out.decode("utf-8"):
#         succeeded = True
#         print(">>> Benchmark completed with success\n")
#     else:
#         print(">>> Benchmark completed with failure\n")

#     # Download reports
#     if copy_report:
#         Benchmark.download_file(output_file, cluster_hostname, ssh_id_file, ssh_user)
#         if succeeded:
#             report_files = [json_report_file, csv_report_file]
#             for report_file in report_files:
#                 Benchmark.download_file(report_file, cluster_hostname, ssh_id_file, ssh_user)
#         print(">>> Downloaded reports to local directory")

# def download_file(file, cluster_hostname, ssh_id_file, ssh_user):
#     run_cmd(f"scp -C -i {ssh_id_file} " +
#             f"{ssh_user}@{cluster_hostname}:{file} {file}",
#             stream_output=True)

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
    parser.add_argument(        
        "--s3-path",
        required=True,
        help="Define the S3 path to store data."
    )
    parser.add_argument(        
        "--master",
        required=True
    )
    parser.add_argument(        
        "--user",
        required=True
    )
    parser.add_argument(        
        "--scale-in-gb",
        required=True
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_command_line_arguments()
    now = datetime.now()
    table_format = args.table_format
    database_name = f"{args.scale_in_gb}gb_{table_format}" 
    experiment_id = now.strftime("%Y%m%d_%H%M%S") + "_" + database_name

    run_cmd(f"rsync -zv run_experiment.py tables.py experiments.py {args.user}@{args.master}:~")
    for o in args.operation.split(","):
        run_cmd_over_ssh(f"""
            spark-submit \\
                --packages {PACKAGES[table_format]} \\
                --py-files experiments.py,tables.py run_experiment.py \\
                --table-format {args.table_format} --operation {o} \\
                --s3-path {args.s3_path} \\
                --scale-in-gb {args.scale_in_gb} \\
                --experiment-id {experiment_id}
        """, "35.89.28.100", "~/.ssh/id_ed25519", args.user)


