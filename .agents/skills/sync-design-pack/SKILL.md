---
name: sync-design-pack
description: Update the CIP architecture design pack (.drawio + generated Markdown) after an agreed design or contract change. Use after approved ADRs, new topics, endpoints, tables or services.
---
# Sync the design pack

The `.drawio` is generated. Do not hand-edit XML and do not hand-edit `docs/design/*.md`.

1. Find the data to change in `tools/design-pack/`: pages 06-08, 14-17 and ADR/assumption tables are in `content1.py`/`content2.py`; diagrams are in `diagrams1.py`/`diagrams2.py` (helpers in `lib.py`).
2. Edit with the smallest change. For a new decision add an ADR row (id, decision, alternatives, trade-off) on page 00 (`content1.py`). Keep naming kebab-case and one abstraction level per diagram; every arrow labelled with protocol or topic.
3. Rebuild and validate: `python tools/design-pack/build.py` (checks XML, dangling arrows, overlapping boxes; must print OK and no size warnings).
4. Regenerate Markdown: `make docs` (= `python tools/drawio_to_md.py docs/architecture/CIP-architecture-design-pack.drawio docs/design`).
5. Cross-check: new/changed topic <-> page 06 <-> schema file; API <-> page 07 <-> OpenAPI; table/key <-> page 08 <-> migration; service <-> spec card <-> a sequence diagram.
6. Report what changed and why; list code that must follow the new design.
