# 302-Retries-and-idempotence

## Description

This repository is a demo to show the concepts of retries, backoff and idempotence when running Flyte task. To have a concrete exemple, the project use a synthetic healthcare dataset [`healthcare_dataset`](https://www.kaggle.com/datasets/prasad22/healthcare-dataset)

The demo intentionally introduces random task failures to observe how Flyte handles retries and how backoff affects the delay between retry attemps. It also demonstrates the difference between idempotent and non-idempotent.

## Project structure
```
302-Retires-and-idempotence
├── data/
│   └── healtcare_dataset.csv      # Concrete data
├── output/
│   └── logs.txt                   # Temporary file for tasks
│   └── result.txt                 # The final result with all tasks
├── src/
│   └── main.py                    # The script where tasks are execute
├── .gitignore
├── .python-version                # The python version
├── pyproject.toml                 # Project dependencies
├── README.md                      # The instruction
└── uv.lock
```

# Requierments

- Python 3.13
- uv

# Setup

Clone the repository
```
git clone https://github.com/rdtjulien/302-Retries-and-idempotence
```

To go in the folder
```
cd 302-Retries-and-idempotence
```

Install the dependencies
```
uv sync
```

To run the project
```
uv run src/main.py
```

# Result

The project runs several Flyte tasks with different configurations.

The results are written to `output/result.txt` and the logs are in the console