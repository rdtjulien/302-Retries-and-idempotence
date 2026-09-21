import random
import flyte
import pandas as pd
from datetime import timedelta
import logging

# Create a Flyte task environment
env = flyte.TaskEnvironment(
    name="retry-idempotent"
)

# Simulate a task that read, process and write data
async def data(idempotent):

    path = "data/healthcare_dataset.csv"
    df = pd.read_csv(path)

    new_df = df[["Name", "Age", "Gender"]].head(3)

    # Overwrite mode for idempotent tasks and append mode for non-idempotent tasks
    m = "a"
    if idempotent:
        m = "w"

    with open("output/logs.txt", m) as f:
        f.write(new_df.to_string())
        f.write("\n\n")

        x = random.randint(0, 1)

        # Simulate a task failure with a 50% chance
        if x == 0:
            print("Task fails")
            f.write("Task fails\n\n\n")
            raise Exception("Task fails")

        f.write("Task success\n")
        print("Task success")

# Define Flyte tasks with different configuration
# Source for flyte task https://www.union.ai/docs/v2/flyte/user-guide/tasks/task-configuration/retries-and-timeouts/
@env.task
async def no_policy():
    await data(False)


@env.task(
    retries=flyte.RetryStrategy(
        count=3,
    ),
)
async def policy():
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
async def policy_backoff():
    await data(False)


@env.task(
    retries=flyte.RetryStrategy(
        count=3,
    ),
)
async def policy_idempotent():
    await data(True)


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

    open("output/result.txt", "w").close()

    for t in [no_policy,policy,policy_backoff,policy_idempotent,policy_idempotent_backoff]:
        name = t.name.split(".")[-1]

        print(f"\n===== {name} =====")
        try:
            await t()
        except Exception:
            pass

        # https://www.geeksforgeeks.org/python/python-copy-contents-of-one-file-to-another-file/
        with open('output/logs.txt','r') as logs, open('output/result.txt','a') as result:
            result.write(f"\n===== {name} =====")
            result.write("\n\n")
            for line in logs:
                    result.write(line)
            result.write("\n\n\n\n")

        open("output/logs.txt", "w").close()


if __name__ == "__main__":
    # Hide flyte log
    flyte.init(log_level=logging.CRITICAL)
    flyte.run(main)