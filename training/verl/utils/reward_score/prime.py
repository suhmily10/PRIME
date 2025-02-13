import asyncio
import json
import traceback
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from typing import Optional
import os
import math

# from .evaluation_utils.code_util import evaluate_code
# from .evaluation_utils.math_util import evaluate_math
from tqdm.asyncio import tqdm
from .evaluation_utils.schema_util import evaluate_schema

from func_timeout import func_set_timeout

@func_set_timeout(10)
def process_completion(completion, reference, task):
    # if task == "code":
        # return evaluate_code(completion, reference)
    # elif task == "math":
        # return evaluate_math(completion, str(reference))
    if task in ["schema","custom","limitation","translation"]:
        return evaluate_schema(completion, reference)
    else:
        raise NotImplementedError(f"Task {task} not implemented.")

    
def process_row_with_timeout(completion, reference, task):
    try:
        return process_completion(completion, reference, task)
    except:
        traceback.print_exc()
        return None


def parallel_evaluate_continual(completions, references, tasks, num_processes, task_timeout=15.0):
    """
    Evaluate rows in parallel with a process pool and timeout handling.
    """
    scores = []
    results = []
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = [executor.submit(process_row_with_timeout, completion, reference, task) for completion, reference, task in zip(completions, references, tasks)]
        
        for future in futures:
            try:
                result = future.result(timeout=task_timeout)
                results.append(result)
            except TimeoutError as e:
                future.cancel()
                results.append(None)
                print("Error: Verifying timeout")
        executor.shutdown(wait=False)

    print('Results:', results)
    # Process results
    for result, completion, reference, task in zip(results, completions, references, tasks):
        if isinstance(result, Exception) or result is None:
            # Handle failed or timed-out tasks
            scores.append(0.0)
            continue

        try:
            # Process result based on task type
            if task == 'code' and not result[0]: # if task is code, the reference should be json string
                correct = 0
                total = min(
                    len(json.loads(reference)['inputs'] if not isinstance(reference, dict) else reference['inputs']),
                    10)
                for run in result[1]:
                    if 'test_case' in run and 'res' in run['test_case'] and run['test_case']['res'] == '[True]':
                        correct += 1
                scores.append(correct / total)
            else:
                if task == "schema":
                    scores.append(float(result))
                elif task == "custom":
                    scores.append(float(result))
                elif task == "limitation":
                    scores.append(float(result))
                elif task == "translation":
                    scores.append(float(result))
                else:
                    scores.append(float(int(result[0])))
        except Exception as e:
            print(f"Error processing result for row: {completion[:10]}, Error: {e}")
            scores.append(0.0)

    return scores
def compute_score(completions, references, tasks):
    # three lists should have identical length
    # TODO: make this one completely asynchronous, which means the main process can do other things(e.g., forwarding reward model) while computing score
    assert len(completions) == len(references) == len(tasks)
    return parallel_evaluate_continual(completions, references, tasks, num_processes=32)
    try:
        return asyncio.run(parallel_evaluate_continual_async(completions, references, tasks, num_processes=32))
    except asyncio.TimeoutError as e:
        print('Global timeout in reward computing! Setting all as 0.5.')
        return [0.5 for _ in range(len(completions))]
