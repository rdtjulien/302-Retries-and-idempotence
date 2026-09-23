import random
import flyte
import pandas as pd
from datetime import timedelta, datetime
import logging

# Create a Flyte task environment
env = flyte.TaskEnvironment(
    name="retry-idempotent"
)

random.seed(22)

# Simulate a task that read, process and write data
async def data(idempotent):
    global last_time
    path = "data/healthcare_dataset.csv"
    df = pd.read_csv(path)

    cancer_count = (df["Medical Condition"] == "Cancer").sum()

    # Read previous count only if this is a retry
    total_cancer_count = cancer_count

    if not idempotent and last_time:
        with open("output/cancer_count.txt", "r") as count_file:
            previous_count = int(count_file.read())
            total_cancer_count += previous_count

    # Save count for the next retry
    with open("output/cancer_count.txt", "w") as count_file:
        count_file.write(str(total_cancer_count))


    now = datetime.now()
    if last_time:
        print(f"retry: {(now - last_time).total_seconds():.1f}s")


    last_time = now

    x = random.randint(0, 1)

    # Simulate a task failure with a 50% chance
    if x == 0:
        print("Task fails")
        raise Exception("Task fails")

    print("Task success")
    print(f"Cancer count: {total_cancer_count}")

# Define Flyte tasks with different configuration
# Source for flyte task https://www.union.ai/docs/v2/flyte/user-guide/tasks/task-configuration/retries-and-timeouts/
@env.task
async def no_retry():
    await data(False)


@env.task(
    retries=flyte.RetryStrategy(
        count=3,
    ),
)
async def retry():
    await data(False)

@env.task(
    retries=flyte.RetryStrategy(
        count=3,
        backoff=flyte.Backoff(
            base=timedelta(seconds=2),
            factor=2.0,
            cap=timedelta(minutes=5),
        ),
    ),
)
async def policy_idempotent_backoff():
    await data(True)

# Run all tasks and collect the result
@env.task
async def main():
    global last_time

    for t in [no_retry,retry,policy_idempotent_backoff]:
        name = t.name.split(".")[-1]
        last_time = None

        print(f"\n===== {name} =====")
        try:
            await t()
        except Exception:
            pass

if __name__ == "__main__":
    # Hide flyte log
    flyte.init(log_level=logging.CRITICAL)
    flyte.run(main)