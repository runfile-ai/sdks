# Example — LangGraph loan-triage (Python)

End-to-end demo: a LangGraph HITL loan-triage agent instrumented with
`runfile-ai`, showing run capture, a human-approval suspension
(`__interrupt__` → `run_suspend` → `awaiting_human`), resume, and a final
decision — then verifying the resulting evidence bundle with `runfile-verifier`.

🚧 Scaffold — to be filled in once the Python LangGraph adapter lands.
