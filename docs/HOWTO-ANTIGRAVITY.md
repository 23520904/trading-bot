# Using the CIP design pack with Antigravity IDE

## 1. Install the kit (5 minutes)
1. Create the repo and copy this kit to its root (keep the folder names).
2. `git init && git add -A && git commit -m "chore: add design pack and agent config"`.
3. Open the folder in Antigravity IDE (open the repo root - rules are discovered from the workspace folder and its parents up to the git root).
4. Click the `...` menu in the agent side panel -> Customizations -> Rules. You should see `AGENTS.md`, `GEMINI.md` and the 8 files from `.agents/rules/`. Skills appear under the Skills tab.
5. Optional: a draw.io extension for the IDE lets you view/edit the `.drawio` visually if it is available in your IDE's extension marketplace.

## 2. What is loaded and when
| File | Loaded | Purpose |
|---|---|---|
| `AGENTS.md` | always | project facts, invariants, how to work (tool-neutral) |
| `GEMINI.md` | always | Antigravity behaviour: language, planning, verification |
| `.agents/rules/*.md` | by `trigger` (`glob`, `model_decision`, `manual`) | per-area detail, loaded only when relevant |
| `.agents/skills/<name>/SKILL.md` | on demand, `/<name>` | repeatable procedures (replace legacy workflows) |

There is no special `RULES.md` file. Per-area rules are the files in `.agents/rules/`; every one must start with YAML frontmatter and a valid snake_case `trigger`, or it is silently ignored. Only files directly inside `.agents/rules/` are scanned.
`AGENTS.md` and `GEMINI.md` are cumulative and always on, so keep them short and never duplicate content between them (24 KB per file, 20,000-token shared budget).

## 3. Why Markdown instead of the .drawio
The `.drawio` is about 290 KB of XML. Agents waste context on it and misread layout. `tools/drawio_to_md.py` converts every page to compact Markdown in `docs/design/` (elements, labelled arrows, tables, sequence order). The agent reads the pages it needs through the index.
Run `make docs` after every design change.

## 4. Daily loop
1. `/start-milestone M0` -> approve the plan.
2. `/implement-service market-collector` (one service or slice at a time).
3. Review the plan artifact, then the diff and the test output. Do not accept "done" without command output.
4. `/design-conformance-review services/market-collector` before merging.
5. If the design must change: agree it, then `/sync-design-pack`.
Parallel agents are fine when slices touch different services and different contracts; never let two agents edit the same schema or migration.

## 5. Starter prompts
- M0: `/start-milestone M0`
- Service: `/implement-service data-validator`
- New field: `/add-or-change-contract add optional field "venue" to MarketTrade`
- Review: `/design-conformance-review services/paper-execution`
- Design change: `/sync-design-pack add ADR-13: ...`

## 6. Pitfalls
- Workflows (`.agents/workflows/`) are legacy and stop being indexed after 1 Nov 2026 - use skills.
- Rules in subfolders of the workspace are not auto-loaded; use `glob` rules in `.agents/rules/` instead of per-service `AGENTS.md`.
- Wrap globs that start with `*` in quotes. Use `model_decision` only with a good `description`.
- Keep rules about invariants and constraints; put multi-step procedures in skills.
- Treat the design pack as the contract: if the agent finds it wrong, it should raise a design question, not work around it.
