import yaml
import shutil
import fileinput
import glob

# This script is used to convert all yaml dag configs to airflow dags
# Can be used by CI/CD for airflow dag deployments
raw_to_staging_dag_template = "/usr/local/airflow/dag_templates/create_raw_to_staging_job.py"
job_files = glob.glob("/usr/local/airflow/dags/config/*")
print(glob.glob("./*"))
print(job_files)
for job in job_files:
    with open(job, "r") as f:
        config = yaml.safe_load(f)
        print("yaml file loaded")
        new_filename = "/usr/local/airflow/dags/" + config["job_name"] + "_job" + ".py"
        if config["job_type"] == "raw_to_staging":
            print(glob.glob("/usr/local/airflow/dag_templates/*"))
            shutil.copyfile(raw_to_staging_dag_template, new_filename)
            with fileinput.input(new_filename, inplace=True) as file:
                for line in file:
                    new_line = (
                        line.replace("input_dag_id", "'" + config["job_name"] + "'")
                        .replace("input_schedule_interval", config["schedule_interval"])
                        .replace("input_job_name", config["job_name"])
                        .replace("input_config_path", job)
                    )
                    print(new_line, end="")
