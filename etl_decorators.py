import functools
import inspect
import json

import pandas as pd
import polars as pl


global INFO_DICT
INFO_DICT = {}


def log_dataframe_info(df):
    """Log details of a DataFrame (compatible with Pandas and Polars)."""
    label = f"DataFrame_{id(df)}"  # Use the unique id of the DataFrame as the label

    if isinstance(df, pd.DataFrame):
        return {
            label: {
                "shape": df.shape,
                "columns": df.columns.tolist()
            }
        }
    elif isinstance(df, pl.DataFrame):
        return {
            label: {
                "shape": (df.height, df.width),  # Polars shape equivalent
                "columns": df.columns  # Polars columns property
            }
        }
    else:
        raise TypeError(f"Unsupported DataFrame type: {type(df).__name__}")


def extract(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)

        args_id = [id(arg) for arg in args]
        args_id = "_".join(map(str, args_id))
        function_id = str(id(func)) + "_" + args_id
        INFO_DICT[function_id] = {}

        INFO_DICT[function_id]["name"] = func.__name__
        INFO_DICT[function_id]["type"] = "load"
        INFO_DICT[function_id]["docstring"] = func.__doc__.replace("\n", " ")
        INFO_DICT[function_id]["input"] = []
        INFO_DICT[function_id]["output"] = []
        INFO_DICT[function_id]["code"] = inspect.getsource(func).strip()

        if isinstance(result, (pd.DataFrame, pl.DataFrame)):
            INFO_DICT[function_id]["output"].append(
                log_dataframe_info(result)
            )
        elif isinstance(result, (tuple, list)):
            for i, item in enumerate(result):
                if isinstance(item, (pd.DataFrame, pl.DataFrame)):
                    INFO_DICT[function_id]["output"].append(
                        log_dataframe_info(item)
                    )

        return result
    return wrapper


def transform(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):

        args_id = [id(arg) for arg in args]
        args_id = "_".join(map(str, args_id))
        function_id = str(id(func)) + "_" + args_id
        INFO_DICT[function_id] = {}

        INFO_DICT[function_id]["name"] = func.__name__
        INFO_DICT[function_id]["type"] = "load"
        INFO_DICT[function_id]["docstring"] = func.__doc__.replace("\n", " ")
        INFO_DICT[function_id]["input"] = []
        INFO_DICT[function_id]["output"] = []
        INFO_DICT[function_id]["code"] = inspect.getsource(func).strip()

        for i, arg in enumerate(args):
            if isinstance(arg, (pd.DataFrame, pl.DataFrame)):
                INFO_DICT[function_id]["input"].append(
                    log_dataframe_info(arg)
                )

        result = func(*args, **kwargs)

        if isinstance(result, (pd.DataFrame, pl.DataFrame)):
            INFO_DICT[function_id]["output"].append(
                log_dataframe_info(result)
            )
        elif isinstance(result, (tuple, list)):
            for i, item in enumerate(result):
                if isinstance(item, (pd.DataFrame, pl.DataFrame)):
                    INFO_DICT[function_id]["output"].append(
                        log_dataframe_info(item)
                    )

        return result
    return wrapper


def load(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):

        args_id = [id(arg) for arg in args]
        args_id = "_".join(map(str, args_id))
        function_id = str(id(func)) + "_" + args_id
        INFO_DICT[function_id] = {}

        INFO_DICT[function_id]["name"] = func.__name__
        INFO_DICT[function_id]["type"] = "load"
        INFO_DICT[function_id]["docstring"] = func.__doc__.replace("\n", " ")
        INFO_DICT[function_id]["code"] = inspect.getsource(func).strip()
        INFO_DICT[function_id]["input"] = []
        INFO_DICT[function_id]["output"] = []

        for i, arg in enumerate(args):
            if isinstance(arg, (pd.DataFrame, pl.DataFrame)):
                INFO_DICT[function_id]["input"].append(
                    log_dataframe_info(arg)
                )

        result = func(*args, **kwargs)

        return result
    return wrapper


def write_json():
    with open("etl_process.json", "w") as f:
        json.dump(INFO_DICT, f, indent=4)


def write_mermaid():
    string = """
```mermaid
graph TD

    """

    for func_id, func_info in INFO_DICT.items():
        for input_df in func_info["input"]:
            for label, info in input_df.items():
                if len(info["columns"]) > 4:
                    columns_string = "\n".join(info["columns"][:3]) + "\n(...)\n " + info["columns"][-1]
                else:
                    columns_string = "\n" + "\n ".join(info["columns"])
                label_columns = str(label) + " " + str(info["shape"]) + columns_string

                label_string = str(label) + '@' + '{ shape: braces, label: "' + label_columns + '" }'
                string += f"{label_string} --> {func_id}[/{func_info["name"]}/]\n"
        for output_df in func_info["output"]:
            for label, info in output_df.items():
                label_string = str(label) + '@' + '{ shape: braces, label: "' + str(label) + '" }'
                string += f"{func_id}[/{func_info["name"]}/] --> {label_string}\n"

    string += "```"

    with open("etl_process.md", "w") as f:
        f.write(string)


def write_prompt():
    prompt_intro = """
I have defined an ETL process in python with a set of functions, i have used some custom decorators to extract information on runtime from these functions, the output is a JSON file with this structure:

structure:
```
{
    [unique id for the function]: {
        "name": [name of the function]",
        "type": [extract, transform, or load],
        "docstring": [docstring of the function],
        "input": [list of input DataFrames],
        "output": [list of output DataFrames],
        "code": [source code of the function]
    },
}
```

Please redact the documentation for the entire ETL process, focusing on the different relationships between the various executions of the functions. The response should be structured with clear sections and titles, each addressing specific aspects of the ETL process. You may use bullet points where necessary to highlight key details or important steps, but the overall tone should be narrative, with long sentences that explain the flow and interconnectedness of the functions. Ensure that the output reflects the complexity of the process while being organized and easy to follow. The documentation should include:

- An overview of the ETL process, summarizing its purpose and main stages.
- A detailed description of each function in the process, emphasizing how they interact and depend on each other.
- A clear explanation of the flow between the functions, detailing how data moves through the process from extraction to final output.
- A conclusion that ties together the entire process, emphasizing the role each function plays in achieving the final result.

output JSON:
```
"""

    prompt_outro = """
```
"""

    prompt = prompt_intro + str(INFO_DICT) + prompt_outro

    with open("etl_process_prompt.md", "w") as f:
        f.write(prompt_intro)
        json.dump(INFO_DICT, f, indent=4)
        f.write(prompt_outro)