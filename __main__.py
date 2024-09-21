import os

import pulumi
import pulumi_docker as docker
import pulumi_aws as aws


def make_airflow_mounted_volumes(client_name):
    # For local volumes, don't need to create a volume
    volumes = [
        docker.ContainerVolumeArgs(
                host_path = os.path.abspath("../airflow"),
                container_path=f'/opt/airflow',
                read_only=False
            )
    ]

    return volumes

def make_management_infra(network: docker.Network):
    postgres_image = docker.get_registry_image(name="postgres:16-bookworm")
    postgres_remote_image = docker.RemoteImage(
        f"airflow_postgres",
        name = postgres_image.name
    )

    postgres_volume = docker.Volume(f"airflow_postgres_volume")

    postgres_container = docker.Container(
        "airflow_postgres",
        name = "airflow_postgres",
        image=postgres_remote_image.name,
        networks_advanced=[docker.ContainerNetworksAdvancedArgs(
            name=network.name)
            ],
        restart = "unless-stopped",
        volumes=[
            docker.ContainerVolumeArgs(
                # target file in the container, not localhost
                container_path="/var/lib/postgresql/data", 
                read_only = False,
                volume_name=postgres_volume.name)
        ],
        envs = [
            # This is for local management of a local airflow DAGs, so db secrets not necessary
            f"POSTGRES_PASSWORD=airflow",
            f"POSTGRES_USER=airflow",
            f"POSTGRES_DB=airflow"
        ], 
        # Choosing a weird port to avoid collisions
        ports = [docker.ContainerPortArgs(internal = 5432, external = POSTGRES_PORT)],
        healthcheck=docker.ContainerHealthcheckArgs(
            tests=["CMD", "pg_isready", "-U", "airflow"],
            interval="10s",
            retries=5,
            start_period="5s",
        ),
        command=["postgres"]
    )


    # Define the Redis service
    redis_image = docker.RemoteImage(
        "airflow-redis-image",
        name="redis:7.2-bookworm"
    )

    redis_container = docker.Container(
        "airflow-redis",
        image=redis_image.name,
        ports=[docker.ContainerPortArgs(
            internal=6379,
            external=6379,
        )],
        networks_advanced=[docker.ContainerNetworksAdvancedArgs(
            name=network.name)
            ],
        healthcheck=docker.ContainerHealthcheckArgs(
            tests=["CMD", "redis-cli", "ping"],
            interval="10s",
            timeout="30s",
            retries=50,
            start_period="30s",
        ),
        restart="unless-stopped"
    )

    pulumi.export("postgres_container_id", postgres_container.id)
    pulumi.export("redis_container_id", redis_container.id)

    return postgres_container, redis_container


def make_airflow_docker(
    client_name: str, 
    airflow_version: str,
    aws_region: str,
    s3_bucket_obj: aws.s3.Bucket, 
    s3_acces_key_obj:  aws.iam.AccessKey
):
    # NETWORK
    network = docker.Network("local-airflow-v1-network")
    pulumi.export("local-airflow-v1-network", network.name)

    # Postgres and Redis for airflow orchestration management
    db_container, redis_container = make_management_infra(network)
    
    # Airflow specific stuff
    airflow_image = docker.RemoteImage(
        "airflow-image",
        name = f"apache/airflow:{airflow_version}"
    )

    airflow_local_volumes = make_airflow_mounted_volumes(client_name = client_name)

    #Shared Env Vars
    common_envs=[
        "AWS_REGION=" + aws_region,
        # The tricky thing about pulumi outputs is you can't encapsulate the apply inside another string
        ## because of the promise nature of pulumi implementation and the lack of await in Python language
        ## Documentation here: https://leebriggs.co.uk/blog/2021/05/09/pulumi-apply
        s3_bucket_obj.bucket.apply(lambda bucket: f"S3_BUCKET_URL=s3://{bucket}"),
        s3_acces_key_obj.id.apply(lambda id: f"AWS_ACCESS_KEY={id}"),
        s3_acces_key_obj.secret.apply(lambda secret: f"AWS_SECRETS_TOKEN={secret}"),
        "AIRFLOW__CORE__EXECUTOR=CeleryExecutor",
        db_container.name.apply(
            lambda name: f"AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@{name}:5432/airflow"
        ),
        db_container.name.apply(lambda name: f"AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@{name}:5432/airflow"),
        redis_container.name.apply(lambda name: f"AIRFLOW__CELERY__BROKER_URL=redis://:@{name}:6379/0"),
        "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true",
        "AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session",
        "AIRFLOW__WEBSERVER__SECRET_KEY:false" # Will change when going live
    ]

    # Specify the Docker image for Airflow   

    # Create the container with the specified configuration  
    airflow_init = docker.Container("airflow-init",  
        image=airflow_image.name,
        entrypoints = ["/bin/bash"],  
        command=[  
            "-c",  
"""  
if [[ -z "${AIRFLOW_UID}" ]]; then  
    echo  
    echo -e "\\033[1;33mWARNING!!!: AIRFLOW_UID not set!\\e[0m"  
    echo "If you are on Linux, you SHOULD follow the instructions below to set "  
    echo "AIRFLOW_UID environment variable, otherwise files will be owned by root."  
    echo "For other operating systems you can get rid of the warning with manually created .env file:"  
    echo "    See: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#setting-the-right-airflow-user"  
    echo  
fi  
one_meg=1048576  
mem_available=$(( $(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE) / one_meg ))  
cpus_available=$(grep -cE 'cpu[0-9]+' /proc/stat)  
disk_available=$(df / | tail -1 | awk '{print $4}')  
warning_resources="false"  
if (( mem_available < 4000 )); then  
    echo  
    echo -e "\\033[1;33mWARNING!!!: Not enough memory available for Docker.\\e[0m"  
    echo "At least 4GB of memory required. You have $(numfmt --to iec $((mem_available * one_meg)))"  
    echo  
    warning_resources="true"  
fi  
if (( cpus_available < 2 )); then  
    echo  
    echo -e "\\033[1;33mWARNING!!!: Not enough CPUS available for Docker.\\e[0m"  
    echo "At least 2 CPUs recommended. You have ${cpus_available}"  
    echo  
    warning_resources="true"  
fi  
if (( disk_available < one_meg * 10 )); then  
    echo  
    echo -e "\\033[1;33mWARNING!!!: Not enough Disk space available for Docker.\\e[0m"  
    echo "At least 10 GBs recommended. You have $(numfmt --to iec $((disk_available * 1024)))"  
    echo  
    warning_resources="true"  
fi  
if [[ ${warning_resources} == "true" ]]; then  
    echo  
    echo -e "\\033[1;33mWARNING!!!: You have not enough resources to run Airflow (see above)!\\e[0m"  
    echo "Please follow the instructions to increase amount of resources available:"  
    echo "   https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#before-you-begin"  
    echo  
fi 
mkdir -p /sources/logs /sources/dags /sources/plugins
chown -R "${AIRFLOW_UID:-0}:0" /sources/{logs,dags,plugins}
echo "Starting airflow here is the version" 
/entrypoint airflow db version
echo "Starting db init"
/entrypoint airflow db init 
echo "finished db init"
/entrypoint airflow db migrate
echo "finished migrate"
exec /entrypoint airflow db check 
echo "Finished"
"""  
        ], 
        networks_advanced=[docker.ContainerNetworksAdvancedArgs(
            name=network.name)
            ], 
        envs=common_envs + [
            "_AIRFLOW_WWW_USER_CREATE=true",
            "_AIRFLOW_WWW_USER_USERNAME=airflow",
            "_AIRFLOW_WWW_USER_PASSWORD=airflow" # WIll change for prod
        ],
        volumes = airflow_local_volumes,
        user="0:0",  
        restart = "on-failure",
        opts=pulumi.ResourceOptions(depends_on=[db_container])
    )  


    # Define the 'airflow-webserver' container
    airflow_webserver = docker.Container(
        'airflow-webserver',
        image=airflow_image.name,  # Assuming airflow_common has the 'image' key
        command=["webserver"],
        networks_advanced=[docker.ContainerNetworksAdvancedArgs(
            name=network.name)
            ],
        restart="unless-stopped",
        volumes = airflow_local_volumes,
        envs = common_envs,
        ports=[docker.ContainerPortArgs(
            internal=8080,
            external=8080,
        )],
        opts=pulumi.ResourceOptions(depends_on=[airflow_init, db_container])
    )

    pulumi.export("airflow-webserver-name", airflow_webserver.name)

    airflow_celery_worker = docker.Container(
        'airflow-celery-worker',
        image=airflow_image.name,  # Assuming airflow_common has the 'image' key
        command=["celery", "worker"],
        networks_advanced=[docker.ContainerNetworksAdvancedArgs(
            name=network.name)
            ],
        healthcheck=docker.ContainerHealthcheckArgs(
            tests=[
                "CMD-SHELL",
                'celery --app airflow.providers.celery.executors.celery_executor.app inspect ping -d "celery@$${HOSTNAME}" || celery --app airflow.executors.celery_executor.app inspect ping -d "celery@$${HOSTNAME}"'
            ],
            interval="30s",
            timeout="10s",
            retries=5,
            start_period="30s"
        ),
        envs = common_envs + ["DUMB_INIT_SETSID=0"],
        restart="unless-stopped",
        volumes = airflow_local_volumes
    )

    pulumi.export("airflow-celery-worker-name", airflow_celery_worker.name)
    
    airflow_scheduler = docker.Container(
        'airflow-scheduler',
        image=airflow_image.name,  # Assuming airflow_common has the 'image' key
        entrypoints = ["/bin/bash"],
        command=["-c", "/entrypoint airflow scheduler"],
        networks_advanced=[docker.ContainerNetworksAdvancedArgs(
            name=network.name)
            ],
        healthcheck=docker.ContainerHealthcheckArgs(
            tests=["CMD", "curl", "--fail", "http://localhost:8974/health"],
            interval="30s",
            timeout="10s",
            retries=5,
            start_period="30s"
        ),
        # Need to add redis name for scheduler backend
        envs = common_envs + [redis_container.name.apply(lambda name: f"DB_HOST={name}")],
        restart="unless-stopped",
        volumes = airflow_local_volumes,
        opts=pulumi.ResourceOptions(depends_on=[airflow_init, redis_container])
    )

    pulumi.export("airflow-worker-scheduler", airflow_scheduler.name)
    
    airflow_triggerer = docker.Container(
        'airflow-triggerer',
        image=airflow_image.name,  # Assuming airflow_common has the 'image' key
        entrypoints = ["/bin/bash"],
        command=["-c", "/entrypoint airflow triggerer"],
        # command=["triggerer"],
        networks_advanced=[docker.ContainerNetworksAdvancedArgs(
            name=network.name)
            ],
        healthcheck=docker.ContainerHealthcheckArgs(
            tests=[
                "CMD-SHELL",
                'airflow jobs check --job-type TriggererJob --hostname "$${HOSTNAME}"'
            ],
            interval="30s",
            timeout="10s",
            retries=5,
            start_period="30s"
        ),
        envs = common_envs,
        restart="unless-stopped",
        volumes = airflow_local_volumes,
        opts=pulumi.ResourceOptions(depends_on=[airflow_init, db_container])
    )

    pulumi.export("airflow-worker-triggerer", airflow_triggerer.name)

    airflow_flower = docker.Container(
        'airflow-flower',
        image=airflow_image.name,  # Assuming airflow_common has the 'image' key
        command=["celery", "flower"],
        networks_advanced=[docker.ContainerNetworksAdvancedArgs(
            name=network.name)
            ],
        healthcheck=docker.ContainerHealthcheckArgs(
            tests=["CMD", "curl", "--fail", "http://localhost:5555/"],
            interval="30s",
            timeout="10s",
            retries=5,
            start_period="30s"
        ),
        ports=[docker.ContainerPortArgs(
            internal=5555,
            external=5555,
        )],
        envs = common_envs,
        restart="unless-stopped",
        volumes = airflow_local_volumes
    )

###################################################################
# Spin everything up
###################################################################
config = pulumi.config()

AWS_REGION = config.require("awsregion")
AIRFLOW_VERSION = config.require("airflowversion")
make_airflow_docker(
    client_name = "Test Client",
    airflow_version = AIRFLOW_VERSION,
    aws_region = AWS_REGION,
    s3_bucket_obj = s3_bucket, # If you need the containers to have access to an S3 Bucket, pass the Pulumi Bucket.
    s3_acces_key_obj = access_secrets
)
