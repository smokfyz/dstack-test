# dstack-test

## Tested on
* macOS 14.3.1
* docker 25.0.3
* python 3.12.3
* poetry 1.8.3

## Prepare

```
poetry shell && poetry install
```

## Usage

```
python main.py --docker-image python --bash-command $'export PYTHONUNBUFFERED=1 && (echo -e \'import time\ncounter = 0\nwhile True:\n\tprint(counter)\n\tcounter = counter + 1\n\ttime.sleep(0.1)\' | python)' --aws-cloudwatch-group test-task-group-1 --aws-cloudwatch-stream test-task-stream-1 --aws-access-key-id ... --aws-secret-access-key ... --aws-region eu-west-2
```
