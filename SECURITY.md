# Security Policy

## Reporting a vulnerability

Please **don't** open a public issue for security vulnerabilities.

Email: **`security@hyerix.ai`**

Include:

- A description of the issue and steps to reproduce it
- The affected version (`hyerix-mcp --version`)
- Any proof-of-concept code or commands
- Whether you've disclosed elsewhere yet

We acknowledge within 2 business days and aim to ship a fix within 30 days for confirmed issues.

## In scope

`hyerix-mcp` is a stdio MCP server that connects to a NATS cluster you control. The credentials and connection URL are supplied by the operator at launch. Issues we treat as security-relevant:

- Anything that lets an MCP client bypass the `--allow-publish` gate
- Anything that lets a tool exceed its server-side bounds (timeout, max-messages, byte caps)
- Memory-safety bugs (panics, crashes, resource exhaustion under malformed input)
- Authentication path bugs (`creds` / NKey / token / user-pass / TLS handling)
- Anything that lets the server leak credentials into logs, error messages, or stdio responses

## Out of scope

- Bugs in upstream `async-nats`, `rmcp`, or `tokio` — please report those to the respective projects
- The behaviour of an LLM that drives this server — we can't constrain how an agent chooses to call us beyond the bounded surface we expose
- Misconfigurations on the operator's side (over-broad NATS credentials, enabling `--allow-publish` in untrusted contexts)

## Coordinated disclosure

If you're a researcher or vendor working under a coordinated-disclosure policy, mention your preferred timeline in the initial email and we'll work with it.
