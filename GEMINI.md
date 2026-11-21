# GEMINI.md - Antigravity agent behaviour for CIP

Project rules, architecture invariants and commands are in `AGENTS.md` (always loaded). This file only adds how the agent should behave in Antigravity. Do not duplicate AGENTS.md here.

## Communication
- Reply to the maintainer in **Vietnamese**. Write code, comments, commit messages, docs and ADRs in **English**.
- Be concise: lead with the result, then the evidence (command output, file paths).

## Planning and artifacts
- Use Planning mode for anything beyond a one-file edit. Produce the plan artifact first: goal, design pages read, files to touch, contracts affected, test plan, risks.
- Keep the task list up to date; mark an item done only after its verification command passed.
- For design questions, stop and ask instead of guessing; give options and a recommendation.

## Context discipline
- Never open `*.drawio` files. Use `docs/design/README.md` and open only the pages you need.
- Do not paste large generated files or logs into the conversation; summarise and cite paths.
- Prefer the skills in `.agents/skills/` (invoke with `/<skill-name>`): `start-milestone`, `implement-service`, `add-or-change-contract`, `design-conformance-review`, `sync-design-pack`.

## Execution
- Run lint, type checks, tests and `make contracts` yourself in the terminal and read the output. Fix failures before reporting done.
- For `web/dashboard` changes, verify in the browser (load the page, check the WebSocket reconnect behaviour and error states) and attach a screenshot or recording as evidence.
- Ask before: installing system packages, changing CI/CD or Kubernetes manifests in `deploy/`, running destructive commands, or touching anything outside this repo.
- Never print or store secrets. If one is needed, ask the maintainer to provide it through the environment.
