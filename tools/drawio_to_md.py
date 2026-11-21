#!/usr/bin/env python3
"""Convert a .drawio file into one agent-readable Markdown file per page.

Usage: python tools/drawio_to_md.py docs/architecture/CIP-architecture-design-pack.drawio docs/design
Why: the raw .drawio XML is huge and wasteful for an AI agent's context window.
     These Markdown files keep names, tech, arrows, labels, tables and sequence order.
Re-run after every change to the .drawio file (CI checks that docs/design is up to date).
"""
import re
import sys
import pathlib
import xml.etree.ElementTree as ET
from html.parser import HTMLParser


class H2M(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.out, self.rows, self.cell, self.row = [], None, None, None

    def _nl(self):
        if self.cell is not None:
            self.cell.append(' / ')
        elif self.rows is None:
            self.out.append('\n')

    def handle_starttag(self, t, a):
        if t == 'table':
            self.rows = []
        elif t == 'tr' and self.rows is not None:
            self.row = []
        elif t in ('td', 'th') and self.rows is not None:
            self.cell = []
        elif t in ('br', 'div', 'p'):
            self._nl()

    def handle_endtag(self, t):
        if t in ('td', 'th') and self.cell is not None:
            self.row.append(re.sub(r'\s+', ' ', ''.join(self.cell)).strip(' /').replace('|', '\\|'))
            self.cell = None
        elif t == 'tr' and self.row is not None:
            self.rows.append(self.row)
            self.row = None
        elif t == 'table' and self.rows is not None:
            r = self.rows
            if r:
                n = max(len(x) for x in r)
                r = [x + [''] * (n - len(x)) for x in r]
                self.out.append('\n\n| ' + ' | '.join(r[0]) + ' |\n|' + '---|' * n + '\n' +
                                '\n'.join('| ' + ' | '.join(x) + ' |' for x in r[1:]) + '\n')
            self.rows = None
        elif t == 'div':
            self._nl()

    def handle_data(self, d):
        if self.cell is not None:
            self.cell.append(d)
        elif self.rows is None:
            self.out.append(d)


def txt(v):
    p = H2M()
    p.feed(v or '')
    s = ''.join(p.out)
    return re.sub(r'\n{3,}', '\n\n', re.sub(r'[ \t]+', ' ', s)).strip()


def one(v):
    return re.sub(r'\s+', ' ', txt(v).replace('\n', ' - ')).strip()


def name(v):
    return (txt(v).split('\n') or [''])[0].strip() or '?'


def geo(c):
    g = c.find('mxGeometry')
    return {k: float(g.get(k, 0)) for k in ('x', 'y', 'width', 'height')} if g is not None else None


def is_participant(c):
    return 'fontStyle=1;' in c.get('style', '') and c.get('vertex') and geo(c) and geo(c)['height'] == 44


def page_md(d):
    cells = list(d.iter('mxCell'))
    by = {c.get('id'): c for c in cells}
    verts = [c for c in cells if c.get('vertex')]
    edges = [c for c in cells if c.get('edge')]
    L = [f"# {d.get('name')}", '', '> Generated from the .drawio file - do not edit by hand.', '']
    bounds = [(c, geo(c)) for c in verts if 'fillColor=none' in c.get('style', '') and 'dashed=1' in c.get('style', '')]
    bset = {id(b) for b, _ in bounds}
    blocks, shapes = [], []
    for c in verts:
        if id(c) in bset or is_participant(c):
            continue
        s, v = c.get('style', ''), c.get('value', '')
        if '<table' in v or ('Courier New' in v and 'word-break' in v) or s.startswith('text;') or 'shape=note' in s:
            blocks.append(c)
        else:
            shapes.append(c)

    def group(c):
        g = geo(c)
        cx, cy = g['x'] + g['width'] / 2, g['y'] + g['height'] / 2
        best = None
        for b, bg in bounds:
            if bg['x'] <= cx <= bg['x'] + bg['width'] and bg['y'] <= cy <= bg['y'] + bg['height']:
                if best is None or bg['width'] * bg['height'] < best[1]:
                    best = (name(b.get('value')), bg['width'] * bg['height'])
        return best[0] if best else ''

    if shapes:
        L += ['## Elements', '']
        cur = None
        for c in sorted(shapes, key=lambda c: (group(c), geo(c)['y'], geo(c)['x'])):
            g = group(c)
            if g != cur:
                if g:
                    L += ['', f'**Group: {g}**', '']
                cur = g
            L.append(f"- {one(c.get('value'))}")
        L.append('')
    named = [e for e in edges if e.get('source') and e.get('target')]
    if named:
        L += ['## Connections (from -> to : label)', '']
        for e in named:
            kind = 'async/Kafka' if 'dashed=1' in e.get('style', '') else 'sync'
            L.append(f"- {name(by[e.get('source')].get('value'))} -> {name(by[e.get('target')].get('value'))} : {one(e.get('value'))} ({kind})")
        L.append('')
    parts = [(c, geo(c)) for c in verts if is_participant(c)]
    msgs = [e for e in edges if not e.get('source') and 'endArrow=none' not in e.get('style', '')]
    if parts and msgs:
        groups = sorted({g['y'] for _, g in parts})
        titles = [(c, geo(c)) for c in verts if c.get('style', '').startswith('text;') and 'fontSize=13;' in c.get('style', '')]
        for i, y0 in enumerate(groups):
            tt = [name(c.get('value')) for c, g in titles if abs(g['y'] - (y0 - 30)) < 2]
            gp = [(name(c.get('value')), g['x'] + g['width'] / 2) for c, g in sorted(parts, key=lambda t: t[1]['x']) if g['y'] == y0]
            L += [f"## Sequence: {tt[0] if tt else y0}", '', 'Participants: ' + ', '.join(n for n, _ in gp), '']
            hi = groups[i + 1] - 30 if i + 1 < len(groups) else 1e9
            rows = []
            for e in msgs:
                pts = {p.get('as'): (float(p.get('x')), float(p.get('y'))) for p in e.iter('mxPoint') if p.get('as')}
                sp, tp = pts['sourcePoint'], pts['targetPoint']
                if sp[1] < y0 or sp[1] >= hi:
                    continue
                f = min(gp, key=lambda q: abs(q[1] - sp[0]))[0]
                t = min(gp, key=lambda q: abs(q[1] - tp[0]))[0]
                rows.append((sp[1], f"- {one(e.get('value'))}  [{f} -> {t}] ({'async' if 'dashed=1' in e.get('style', '') else 'sync'})"))
            L += [r for _, r in sorted(rows)] + ['']
    if blocks:
        L += ['## Tables, notes and text blocks', '']
        for c in sorted(blocks, key=lambda c: (geo(c)['y'], geo(c)['x'])):
            t = txt(c.get('value'))
            if t and not t.startswith('Legend'):
                L += [t, '']
    return '\n'.join(L).rstrip() + '\n'


def main(src, out):
    out = pathlib.Path(out)
    out.mkdir(parents=True, exist_ok=True)
    root = ET.parse(src).getroot()
    idx = ['# Design pack index (generated)', '', 'Read the page you need, not the whole pack. Source of truth: the .drawio file.', '']
    n = 0
    for d in root.findall('diagram'):
        fn = re.sub(r'[^a-z0-9]+', '-', d.get('name').lower()).strip('-') + '.md'
        (out / fn).write_text(page_md(d), encoding='utf-8')
        idx.append(f"- [{d.get('name')}]({fn})")
        n += 1
    (out / 'README.md').write_text('\n'.join(idx) + '\n', encoding='utf-8')
    print(f'wrote {n} pages to {out}')


if __name__ == '__main__':
    main(*sys.argv[1:3])
