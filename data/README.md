# Data Directory

This directory contains conversation data for the call center analytics pipeline.

## Structure

- `conversations/` - Place your conversation JSONL files here
  - Format: `conversation_1.jsonl`, `conversation_2.jsonl`, etc.
  - Each file contains a call conversation with alternating Expert/Subscriber messages

- `conversations_output/` - Place your feature extraction output here
  - Format: Same filename as conversation files
  - Contains JSON with extracted features (sentiment, bad words, summaries, etc.)

## Example Files

### conversations/conversation_1.jsonl
```jsonl
{"Expert": "Hello, how can I help you today?"}
{"Subscriber": "I need help with my account"}
{"Expert": "I'd be happy to help. Can you provide your account number?"}
{"Subscriber": "Sure, it's 12345"}
{"Expert": "Thank you. Let me check that for you."}
{"Subscriber": "Thanks"}
{"Expert": "Your account is active. Is there anything else I can help with?"}
{"Subscriber": "No, that's all. Thank you!"}
{"Expert": "You're welcome! Have a great day."}
```

### conversations_output/conversation_1.jsonl
```json
{
  "sentiment_analysis": {
    "expert": {"class": "NORMAL"},
    "subscriber": {"class": "POSITIVE"}
  },
  "bad_words": {
    "expert": {"class": false},
    "subscriber": {"class": false}
  },
  "start_greeting": {
    "expert": {"class": true}
  },
  "end_greeting": {
    "farewell": {"class": true},
    "reference": {"class": false}
  },
  "summary": {
    "contact_reason_summary": "Account status inquiry",
    "dialogue_summary": "Customer inquired about account status. Agent confirmed account is active."
  }
}
```

## Getting Started

1. Place your conversation files in `conversations/`
2. Place corresponding feature files in `conversations_output/`
3. Run: `python main.py ingest`
