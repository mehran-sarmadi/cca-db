from datetime import datetime, timedelta
import json
from typing import Dict, List

def pg_calls_analysis_rows() -> List[Dict]:
    rows = [
        {
            "sentiment_analysis": "positive",
            "bad_words": "[]",
            "start_greeting": True,
            "end_greeting": True,
            "summary": "Quick, helpful call.",
        },
        {
            "sentiment_analysis": "neutral",
            "bad_words": '["oops"]',
            "start_greeting": True,
            "end_greeting": False,
            "summary": "Issue unresolved.",
        },
        {
            "sentiment_analysis": "negative",
            "bad_words": '["bad","worse"]',
            "start_greeting": False,
            "end_greeting": False,
            "summary": "Customer was upset.",
        },
    ]
    return rows

def pg_call_text_json(call_id: int) -> str:
    convo = {
        "call_id": call_id,
        "turns": [
            {"speaker": "customer", "text": "Hello"},
            {"speaker": "agent", "text": "Hi! How can I help?"},
            {"speaker": "customer", "text": "Billing issue."},
        ],
    }
    return json.dumps(convo)

def ch_selected_calls_details_rows(start: datetime, n: int = 5) -> List[Dict]:
    rows: List[Dict] = []
    for i in range(n):
        ts = start + timedelta(hours=i)
        rows.append(
            {
                "call_id": 1000 + i,
                "subscriber_id": 200 + (i % 2),
                "expert_id": 10 + (i % 3),
                "call_timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "duration_seconds": 60 * (i + 1),
                "features": json.dumps(
                    {
                        "topic": ["billing", "tech", "general"][i % 3],
                        "score": round(0.5 + 0.1 * i, 2),
                        "flag": i % 2 == 0,
                    }
                ),
            }
        )
    return rows

def ch_experts_rows() -> List[Dict]:
    return [
        {"expert_id": 10, "name": "Alice"},
        {"expert_id": 11, "name": "Bob"},
        {"expert_id": 12, "name": "Carol"},
    ]
