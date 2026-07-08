# report_pg.py
import psycopg2
import json
from datetime import datetime
from typing import Any
from pytestflow.core.pytestflow_states import PyTestflowState
from pytestflow.core.context import ptf_context
import uuid 

def _extract_result(state):
    return getattr(state, "result", None)

def _extract_children(state):
    return getattr(state, "children", []) or []

def _extract_ptf_result(state):
    return getattr(state, "ptf_result", {})

def _extract_status_name(state):
    return getattr(state, "name", state.__class__.__name__)

def _extract_message(state):
    return getattr(state, "message", "") or ""

def _walk_all_states(root: PyTestflowState):
    """Walk the tree and yield all (path, state) pairs"""
    def _walk(state, path):
        yield (path, state)
        for i, (name, child) in enumerate(_extract_children(state), 1):
            yield from _walk(child, path + [name])
    return list(_walk(root, []))

def log_results_to_db_old(root_state: PyTestflowState, db_conn_str: str):
    conn = psycopg2.connect(db_conn_str)
    cur = conn.cursor()

    ptf_result = _extract_ptf_result(root_state)
    flow_run_id = ptf_result.get("flow_run_id")
    flow_name = ptf_result.get("flow_name")
    start_time = ptf_result.get("start_time", datetime.utcnow().isoformat())
    end_time = datetime.utcnow().isoformat()
    status = _extract_status_name(root_state)

    # Insert test session
    session_uuid = str(uuid.uuid4())
    cur.execute("""
        INSERT INTO test_sessions (uuid, flow_run_id, flow_name, start_time, end_time, status)
        VALUES (%s, %s, %s, %s, %s, %s)
                """, 
        (session_uuid, flow_run_id, flow_name, start_time, end_time, status))

    # Insert DUT run (you may later want to link to test_sessions via a join on context_id)
    cur.execute("""
        INSERT INTO dut_runs (
            context_id, dut_sn, main_sequence, result
        )
        VALUES (%s, %s, %s, %s)
        RETURNING id
    """, (
        ptf_result.get("context_id"),  # optional
        ptf_result.get("dut_sn", "UNKNOWN"),
        ptf_result.get("main_sequence", "unknown"),
        status
    ))
    dut_run_id = cur.fetchone()[0]

    # Insert step results into callable_runs
    for path, state in _walk_all_states(root_state):
        res = _extract_ptf_result(state)
        step_name = res.get("step_name", ".".join(str(p) for p in path))
        step_type = res.get("step_type", "unknown")
        step_version = res.get("step_version", None)
        result_value = _extract_result(state)
        status = _extract_status_name(state)
        duration_ms = res.get("duration_ms", None)
        timestamp = res.get("start_time", datetime.utcnow().isoformat())
        additional_info = res.get("additional_info", {})

        cur.execute("""
            INSERT INTO callable_runs (
                dut_run_id, step_name, step_type, step_version, flow_run_id,
                status, result, additional_info, start_time, duration_ms
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            dut_run_id,
            step_name,
            step_type,
            step_version,
            flow_run_id,
            status,
            json.dumps(result_value, default=str),
            json.dumps(additional_info),
            timestamp,
            duration_ms
        ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Results logged to DB: session_id={session_id}, dut_run_id={dut_run_id}")


def log_results_to_db(root_state: PyTestflowState, db_conn_str: str):
    conn = psycopg2.connect(db_conn_str)
    cur = conn.cursor()

    ptf_result = _extract_ptf_result(root_state)
    flow_run_id = ptf_result.get("flow_run_id")
    flow_name = ptf_result.get("flow_name")
    start_time = ptf_result.get("start_time", datetime.utcnow().isoformat())
    end_time = datetime.utcnow().isoformat()
    status = _extract_status_name(root_state)

    # Additional session metadata from pre_uut_sequence (optional)
    title =  ptf_context.locals["__caller__"].get("title", None)
    operator = ptf_context.locals["__caller__"].get("operator", None)
    bench = ptf_context.locals["__caller__"].get("bench", None)
    process_model = ptf_context.locals["__caller__"].get("process_model", None)

    # Insert test session
    session_uuid = str(uuid.uuid4())
    cur.execute("""
        INSERT INTO test_sessions (
            uuid, flow_run_id, flow_name, start_time, end_time, status,
            title, operator, bench, process_model
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        session_uuid,
        flow_run_id,
        flow_name,
        start_time,
        end_time,
        status,
        title,
        operator,
        bench,
        process_model
    ))

    # Insert DUT run (link to context if exists)
    cur.execute("""
        INSERT INTO dut_runs (
            context_id, dut_sn, main_sequence, result
        )
        VALUES (%s, %s, %s, %s)
        RETURNING id
    """, (
        ptf_result.get("context_id"),
        ptf_result.get("dut_sn", "UNKNOWN"),
        ptf_result.get("main_sequence", "unknown"),
        status
    ))
    dut_run_id = cur.fetchone()[0]

    # Insert step results
    for path, state in _walk_all_states(root_state):
        res = _extract_ptf_result(state)
        step_name = res.get("step_name", ".".join(str(p) for p in path))
        step_type = res.get("step_type", "unknown")
        step_version = res.get("step_version", None)
        result_value = _extract_result(state)
        status = _extract_status_name(state)
        duration_ms = res.get("duration_ms", None)
        timestamp = res.get("start_time", datetime.utcnow().isoformat())
        additional_info = res.get("additional_info", {})

        cur.execute("""
            INSERT INTO callable_runs (
                dut_run_id, step_name, step_type, step_version, flow_run_id,
                status, result, additional_info, start_time, duration_ms
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            dut_run_id,
            step_name,
            step_type,
            step_version,
            flow_run_id,
            status,
            json.dumps(result_value, default=str),
            json.dumps(additional_info),
            timestamp,
            duration_ms
        ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Results logged to DB: session_uuid={session_uuid}, dut_run_id={dut_run_id}")
