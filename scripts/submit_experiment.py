
import os
import shlex
import subprocess
import argparse
import time
from datetime import datetime

SCALA_VERSION = "2.12"
ICEBERG_VERSION = "0.14.0"
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
    full_cmd = f"""ssh -i {ssh_id_file} {user}@{host} '{cmd}'"""
    return run_cmd(full_cmd, **kwargs)

def wait_and_download_results(master, ssh_id_file, experiment_id, ssh_user, operation):
    completed = False

    results_file = f"{experiment_id}_{operation}_results.csv"
    out_file = f"{experiment_id}_{operation}.out"
    while not completed:
        print(f"Waiting for completion of the experiment id {experiment_id}_{operation}...")
        (_, out, _) = run_cmd_over_ssh(f"ls {results_file}", master, ssh_id_file, ssh_user,
                                        throw_on_error=False)
        if results_file in out.decode("utf-8"):
            completed = True
        else:
            time.sleep(30)
    
    run_cmd(f"rsync -zv {ssh_user}@{master}:~/{results_file} .")
    run_cmd(f"rsync -zv {ssh_user}@{master}:~/{out_file} .")
    print("Downloaded results file.")


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

    ssh_file = "~/.ssh/id_ed25519"
    master = args.master
    user = args.user
    scale_in_gb = args.scale_in_gb

    run_cmd(f"rsync -zv run_experiment.py tables.py experiments.py {user}@{master}:~")
    for tf in table_format.split(","):
        for s in scale_in_gb.split(","):
            database_name = f"{s}gb_{tf}" 
            experiment_id = now.strftime("%Y%m%d_%H%M%S") + "_" + database_name
            for o in args.operation.split(","):
                print(f"Running {experiment_id}...")
                run_cmd_over_ssh((
                    "screen -d -m bash -c "
                    "\"spark-submit "
                        f"--packages {PACKAGES[tf]} "
                        f"--py-files experiments.py,tables.py run_experiment.py "
                        f"--table-format {tf} --operation {o} "
                        f"--s3-path {args.s3_path} "
                        f"--scale-in-gb {s} "
                        f"--experiment-id {experiment_id} "
                    f"&> {experiment_id}_{o}.out\""
                ), master, ssh_file, user)
                wait_and_download_results(master, ssh_file, experiment_id, user, o)



