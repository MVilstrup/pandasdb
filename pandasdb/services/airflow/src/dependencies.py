from airflow.sensors.external_task import ExternalTaskSensor


def edge_to(child_id):
    child_name = ".".join(child_id.split(".")[-2:])
    return f"NOTIFY--{child_name}"


def edge_from(parent_id):
    parent_name = ".".join(parent_id.split(".")[-2:])
    return f"WAIT_FOR--{parent_name}"


def subscribe_to(parent_dag_ids, timeout=3600, allowed_states=['success'], failed_states=['failed'], mode="reschedule"):
    tasks = []
    for parent_id in parent_dag_ids:
        tasks.append(ExternalTaskSensor(
            task_id=edge_from(parent_id),
            external_dag_id=parent_id,
            external_task_id=None,
            timeout=timeout,
            check_existence=True,
            allowed_states=allowed_states,
            failed_states=failed_states,
            mode=mode,
        ))
    return tasks
