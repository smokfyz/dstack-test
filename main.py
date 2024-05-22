import argparse
import logging
import queue
import signal
import threading
import time
import typing

import boto3
import docker
from docker.models.containers import Container

logging.basicConfig(level=logging.INFO)

log = logging.getLogger(__name__)


LOGS_QUEUE_SIZE = 50


class DockerManager:
    def __init__(self) -> None:
        self.client = docker.from_env()
        self.container = None

    def run_container(
        self, image_name: str, command: str, log_callback: typing.Callable[[str], None]
    ) -> None:
        command = f"/bin/bash -c \"{command}\""
        log.info(f"Running container with image: {image_name} and command: {command}")
        self.container = self.client.containers.run(
            image_name, command, detach=True, stdout=True, stderr=True
        )
        log.info(f"Container started with ID: {self.container.id}")

        log_thread = threading.Thread(
            target=self.collect_logs, args=(self.container, log_callback)
        )
        log_thread.start()

        log.info("Waiting for container to finish...")
        self.container.wait()
        log_thread.join()

    def collect_logs(
        self, container: Container, log_callback: typing.Callable[[str], None]
    ) -> None:
        for line in container.logs(stream=True):
            log_callback(line.decode('utf-8'))

    def kill(self) -> None:
        if self.container is not None:
            log.info("Killing container...")
            self.container.kill()

    def remove(self) -> None:
        if self.container is not None:
            log.info("Removing container...")
            self.container.remove()


class CloudWatchManager:
    def __init__(
        self, aws_access_key_id: str, aws_secret_access_key: str, region_name: str
    ) -> None:
        self.client = boto3.client(
            'logs',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )

    def create_log_group(self, group_name: str) -> None:
        try:
            self.client.create_log_group(logGroupName=group_name)
            log.info(f"Created CloudWatch log group: {group_name}")
        except self.client.exceptions.ResourceAlreadyExistsException:
            log.info(f"Log group {group_name} already exists.")

    def create_log_stream(self, group_name: str, stream_name: str) -> None:
        try:
            self.client.create_log_stream(
                logGroupName=group_name, logStreamName=stream_name
            )
            log.info(
                f"Created CloudWatch log stream: {stream_name} in group: {group_name}"
            )
        except self.client.exceptions.ResourceAlreadyExistsException:
            log.info(f"Log stream {stream_name} already exists in group: {group_name}")

    def send_logs(
        self, group_name: str, stream_name: str, log_events: typing.List[dict]
    ) -> None:
        log.info(f"Sending {len(log_events)} logs to CloudWatch...")
        try:
            self.client.put_log_events(
                logGroupName=group_name,
                logStreamName=stream_name,
                logEvents=log_events,
            )
        except Exception as e:
            log.error(f"Failed to send logs to CloudWatch: {e}")


class LogHandler:
    def __init__(
        self, cloudwatch_manager: CloudWatchManager, group_name: str, stream_name: str
    ) -> None:
        self.cloudwatch_manager = cloudwatch_manager
        self.group_name = group_name
        self.stream_name = stream_name
        self.log_events_queue = queue.Queue(maxsize=LOGS_QUEUE_SIZE)
        self.flush_thread = threading.Thread(target=self.flush_logs)
        self.finished = threading.Event()

        self.flush_thread.start()

    def flush_logs(self) -> None:
        while not self.finished.is_set() or not self.log_events_queue.empty():
            log_events = []
            for _ in range(self.log_events_queue.qsize()):
                try:
                    log_event = self.log_events_queue.get(block=False)
                    log_events.append(log_event)
                except queue.Empty:
                    break
            if len(log_events) != 0:
                self.cloudwatch_manager.send_logs(
                    self.group_name, self.stream_name, log_events
                )

    def handle_log(self, log_message: str) -> None:
        log_event = {'timestamp': int(time.time() * 1000), 'message': log_message}
        self.log_events_queue.put(log_event)

    def stop(self) -> None:
        self.finished.set()
        self.flush_thread.join()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a Docker container and send logs to AWS CloudWatch."
    )
    parser.add_argument(
        '--docker-image', required=True, help='Name of the Docker image'
    )
    parser.add_argument(
        '--bash-command',
        required=True,
        help='Bash command to run inside the Docker container',
    )
    parser.add_argument(
        '--aws-cloudwatch-group',
        required=True,
        help='Name of the AWS CloudWatch log group',
    )
    parser.add_argument(
        '--aws-cloudwatch-stream',
        required=True,
        help='Name of the AWS CloudWatch log stream',
    )
    parser.add_argument('--aws-access-key-id', required=True, help='AWS access key ID')
    parser.add_argument(
        '--aws-secret-access-key', required=True, help='AWS secret access key'
    )
    parser.add_argument('--aws-region', required=True, help='AWS region')

    args = parser.parse_args()

    try:
        cloudwatch_manager = CloudWatchManager(
            aws_access_key_id=args.aws_access_key_id,
            aws_secret_access_key=args.aws_secret_access_key,
            region_name=args.aws_region,
        )
        cloudwatch_manager.create_log_group(args.aws_cloudwatch_group)
        cloudwatch_manager.create_log_stream(
            args.aws_cloudwatch_group, args.aws_cloudwatch_stream
        )

        log_handler = LogHandler(
            cloudwatch_manager, args.aws_cloudwatch_group, args.aws_cloudwatch_stream
        )
        docker_manager = DockerManager()

        def shutdown_handler(sig: typing.Any, frame: typing.Any) -> None:
            log.info("Shutting down...")
            docker_manager.kill()
            log_handler.stop()

        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        docker_manager.run_container(
            args.docker_image, args.bash_command, log_handler.handle_log
        )
        docker_manager.remove()
    except Exception as e:
        log.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
