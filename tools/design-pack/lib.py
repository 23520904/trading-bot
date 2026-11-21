import math
from xml.sax.saxutils import escape, quoteattr

WARN = []
COL = {'blue': ('#dae8fc', '#6c8ebf'), 'orange': ('#ffe6cc', '#d79b00'), 'purple': ('#e1d5e7', '#9673a6'),
       'green': ('#d5e8d4', '#82b366'), 'grey': ('#f5f5f5', '#666666')}
EB = 'html=1;endArrow=block;endFill=1;strokeColor=#333333;strokeWidth=1.5;fontSize=10;'
ORTH = 'edgeStyle=orthogonalEdgeStyle;rounded=0;jettySize=auto;labelBackgroundColor=#FFFFFF;'
DASH = 'dashed=1;dashPattern=6 4;'
EDGE = {'sync': ORTH + EB, 'async': ORTH + EB + DASH,
        'seq': EB + 'verticalAlign=bottom;align=center;',
        'seqa': EB + 'verticalAlign=bottom;align=center;' + DASH,
        'life': 'html=1;endArrow=none;dashed=1;strokeColor=#aaaaaa;'}
TYP = {'svc': 'Container', 'eng': 'Job', 'db': 'Datastore', 'ext': 'External System', 'mq': 'Container'}


def esc(s): return escape(str(s))
def hx(s): return esc(s).replace('\n', '<br>')


class Page:
    def __init__(s, idx, name):
        s.idx, s.name = idx, name
        s.cells, s.n, s.ids, s.nodes = [], 0, {}, []
        s.mx = s.my = 0

    def _id(s):
        s.n += 1
        return f"p{s.idx}_{s.n}"

    def vertex(s, x, y, w, h, label, style, key=None, node=False, track=True):
        i = s._id()
        s.cells.append(f'<mxCell id="{i}" value={quoteattr(label)} style="{style}" vertex="1" parent="1">'
                       f'<mxGeometry x="{x}" y="{y}" width="{w}" height="{h}" as="geometry"/></mxCell>')
        if track:
            s.mx, s.my = max(s.mx, x + w), max(s.my, y + h)
        if key:
            assert key not in s.ids, f"dup key {key} on page {s.name}"
            s.ids[key] = i
        if node:
            s.nodes.append((key or i, x, y, w, h))
        return i

    def edge(s, a=None, b=None, label='', kind='sync', style='', pts=None, sp=None, tp=None):
        i = s._id()
        src = f' source="{s.ids[a]}"' if a else ''
        tgt = f' target="{s.ids[b]}"' if b else ''
        g = ''
        if sp: g += f'<mxPoint x="{sp[0]}" y="{sp[1]}" as="sourcePoint"/>'
        if tp: g += f'<mxPoint x="{tp[0]}" y="{tp[1]}" as="targetPoint"/>'
        if pts:
            g += '<Array as="points">' + ''.join(f'<mxPoint x="{x}" y="{y}"/>' for x, y in pts) + '</Array>'
        s.cells.append(f'<mxCell id="{i}" value={quoteattr(hx(label))} style="{EDGE[kind]}{style}" edge="1" parent="1"{src}{tgt}>'
                       f'<mxGeometry relative="1" as="geometry">{g}</mxGeometry></mxCell>')
        return i

    def xml(s):
        W, H = int(s.mx + 60), int(s.my + 60)
        return (f'<diagram id="d{s.idx}" name={quoteattr(s.name)}><mxGraphModel dx="1400" dy="900" grid="1" gridSize="10" guides="1" '
                f'tooltips="1" connect="1" arrows="1" fold="1" page="1" pageScale="1" pageWidth="{W}" pageHeight="{H}" math="0" shadow="0">'
                f'<root><mxCell id="0"/><mxCell id="1" parent="0"/>{"".join(s.cells)}</root></mxGraphModel></diagram>')


def title(p, t, sub):
    p.vertex(20, 10, 1800, 60, f'<b style="font-size:20px">{esc(t)}</b><br><span style="font-size:11px;color:#555555">{esc(sub)}</span>',
             'text;html=1;align=left;verticalAlign=top;whiteSpace=wrap;overflow=hidden;', track=False)


def legend(p, x, y, flow='Arrow = caller -> callee'):
    def sw(c):
        f, st = COL[c]
        return f'<td bgcolor="{f}" style="border:1px solid {st};width:26px;">&nbsp;</td>'
    rows = [(sw('blue'), 'Application container (own code)'), (sw('orange'), 'Processing engine job (Flink / Spark / Airflow)'),
            (sw('purple'), 'Kafka cluster / topic (pill)'), (sw('green'), 'Datastore (cylinder)'),
            ('<td style="border:1px dashed #666666;width:26px;">&nbsp;</td>', 'External system (dashed)'),
            ('<td align="center">&#9472;&#9472;&#9654;</td>', 'Synchronous call / connection'),
            ('<td align="center">&#9480;&#9480;&#9654;</td>', 'Asynchronous / event via Kafka')]
    h = ('<b>Legend</b><table cellspacing="2" cellpadding="1" style="font-size:10px;">' +
         ''.join(f'<tr>{a}<td>{esc(b)}</td></tr>' for a, b in rows) +
         f'</table><span style="font-size:10px;color:#555555">{esc(flow)}</span>')
    p.vertex(x, y, 330, 190, h, 'rounded=0;whiteSpace=wrap;html=1;fillColor=#ffffff;strokeColor=#999999;align=left;verticalAlign=top;spacing=6;')


def actor(p, key, x, y, label):
    p.vertex(x, y, 36, 70, esc(label), 'shape=umlActor;verticalLabelPosition=bottom;verticalAlign=top;html=1;outlineConnect=0;fontSize=11;fillColor=#ffffff;strokeColor=#333333;',
             key=key, node=True)


def node(p, key, kind, x, y, name, tech='', resp='', w=230, h=90, color=None, typ=None):
    color = color or {'svc': 'blue', 'eng': 'orange', 'topic': 'purple', 'db': 'green', 'ext': 'grey', 'mq': 'purple'}[kind]
    f, st = COL[color]
    base = f'whiteSpace=wrap;html=1;fillColor={f};strokeColor={st};fontSize=11;fontColor=#1a1a1a;'
    if kind == 'db': style = base + 'shape=cylinder3;boundedLbl=1;backgroundOutline=1;size=10;'
    elif kind == 'topic': style = base + 'rounded=1;arcSize=50;fontFamily=Courier New;fontSize=10;'
    elif kind == 'ext': style = base + 'rounded=1;arcSize=8;dashed=1;'
    else: style = base + 'rounded=1;arcSize=8;'
    cpl = max(10, int((w - 16) / 5.3))
    lab = f'<b>{esc(name)}</b>'
    lines = 1
    if kind == 'topic':
        if tech:
            lab += f'<br><span style="font-size:9px">{esc(tech)}</span>'
            lines += 1
    else:
        t = TYP.get(kind, '') if typ is None else typ
        inner = f'{t}: {tech}' if (t and tech) else (t or tech)
        if inner:
            lab += f'<br><span style="font-size:10px;color:#444444">[{esc(inner)}]</span>'
            lines += math.ceil(len(inner) / cpl)
        if resp:
            lab += f'<br><span style="font-size:10px;color:#222222">{esc(resp)}</span>'
            lines += math.ceil(len(resp) / cpl)
    need = lines * 13.5 + 12 + (14 if kind == 'db' else 0)
    if need > h:
        WARN.append(f'[{p.name}] node {key}: needs ~{int(need)}px, has {h}')
    return p.vertex(x, y, w, h, lab, style, key=key, node=True)


def boundary(p, x, y, w, h, t, key=None):
    return p.vertex(x, y, w, h, esc(t), 'rounded=0;whiteSpace=wrap;html=1;fillColor=none;dashed=1;strokeColor=#888888;verticalAlign=top;align=left;'
                    'spacingLeft=10;spacingTop=4;fontStyle=1;fontSize=12;fontColor=#444444;', key=key)


def note(p, x, y, w, h, html_text, key=None):
    return p.vertex(x, y, w, h, html_text, 'shape=note;whiteSpace=wrap;html=1;backgroundOutline=1;darkOpacity=0.05;fillColor=#fff2cc;strokeColor=#d6b656;'
                    'align=left;verticalAlign=top;spacing=6;fontSize=10;', key=key)


def tbl(p, x, y, w, ttl, headers, rows, widths=None, fs=10, mono=False, hdr='#dae8fc'):
    n = len(headers)
    widths = widths or [1] * n
    tot = sum(widths)
    pct = [wd / tot for wd in widths]
    wpx = [w * q for q in pct]
    cw = fs * (0.62 if mono else 0.56)

    def rh(cells):
        L = 1
        for c, wp in zip(cells, wpx):
            per = max(1, int((wp - 8) / cw))
            L = max(L, sum(max(1, math.ceil(len(t) / per)) for t in str(c).split('\n')))
        return L * (fs + 3) + 8
    height = 30 + rh(headers) + sum(rh(r) for r in rows)
    ff = 'font-family:Courier New;' if mono else ''
    h = f'<div style="font-size:13px;font-weight:bold;padding:2px 0 4px 0;">{esc(ttl)}</div>'
    h += f'<table border="1" cellpadding="3" cellspacing="0" style="border-collapse:collapse;border-color:#999999;font-size:{fs}px;{ff}width:100%;table-layout:fixed;">'
    h += f'<tr style="background:{hdr};font-weight:bold;">' + ''.join(f'<td valign="top" width="{q * 100:.1f}%">{hx(c)}</td>' for c, q in zip(headers, pct)) + '</tr>'
    for r in rows:
        h += '<tr>' + ''.join(f'<td valign="top" style="word-wrap:break-word;">{hx(c)}</td>' for c in r) + '</tr>'
    h += '</table>'
    p.vertex(x, y, w, height, h, 'text;html=1;strokeColor=none;fillColor=none;align=left;verticalAlign=top;whiteSpace=wrap;overflow=visible;')
    return y + height + 30


def code(p, x, y, w, ttl, text, fs=10):
    cw = fs * 0.62
    per = max(10, int((w - 20) / cw))
    lines = sum(max(1, math.ceil(len(l) / per)) for l in text.split('\n'))
    h = 34 + lines * (fs + 3) + 14
    body = ''
    for l in text.split('\n'):
        t = l.lstrip(' ')
        body += '&nbsp;' * (len(l) - len(t)) + esc(t) + '<br>'
    lab = (f'<b style="font-size:13px">{esc(ttl)}</b><br><div style="font-family:Courier New;font-size:{fs}px;word-break:break-all;">{body}</div>')
    p.vertex(x, y, w, h, lab, 'rounded=0;whiteSpace=wrap;html=1;fillColor=#fafafa;strokeColor=#cccccc;align=left;verticalAlign=top;spacing=8;overflow=visible;')
    return y + h + 30


def ns_height(w, items, fs=10):
    per = int((w - 28) / (fs * 0.56))
    return 38 + sum(max(1, math.ceil(len(i) / per)) for i in items) * (fs + 3) + 14


def nsbox(p, x, y, w, ttl, items, color='blue', h=None, key=None, fs=10):
    h = h or ns_height(w, items, fs)
    f, st = COL[color]
    lab = f'<b style="font-size:12px">{esc(ttl)}</b>' + ''.join(
        f'<div style="text-align:left;font-size:{fs}px;padding-left:10px;text-indent:-10px;">&bull; {esc(i)}</div>' for i in items)
    p.vertex(x, y, w, h, lab, f'rounded=0;whiteSpace=wrap;html=1;fillColor={f};strokeColor={st};dashed=1;align=left;verticalAlign=top;spacing=8;spacingTop=4;', key=key)
    return h


def sequence(p, x0, y0, ttl, parts, msgs, pitch=185, w=150, step=34):
    p.vertex(x0, y0 - 30, len(parts) * pitch, 24, f'<b>{esc(ttl)}</b>', 'text;html=1;align=left;verticalAlign=middle;fontSize=13;')
    cx = {}
    H = 44 + step * (len(msgs) + 1)
    for i, (k, lab, col) in enumerate(parts):
        x = x0 + i * pitch
        f, st = COL[col]
        p.vertex(x, y0, w, 44, esc(lab), f'rounded=1;whiteSpace=wrap;html=1;fillColor={f};strokeColor={st};fontSize=10;fontStyle=1;', node=True)
        cx[k] = x + w / 2
        p.edge(None, None, '', 'life', sp=(cx[k], y0 + 44), tp=(cx[k], y0 + H))
    for j, (a, b, t, k) in enumerate(msgs):
        y = y0 + 44 + step * (j + 1)
        p.edge(None, None, f'{j + 1}. {t}', 'seqa' if k == 'a' else 'seq', sp=(cx[a], y), tp=(cx[b], y))
    return y0 + H + 70


def comp(p, x0, y0, ttl, cols, rows, nodes, edges, pitchx=265, pitchy=130, w=195, h=76):
    boundary(p, x0, y0, cols * pitchx + 20, rows * pitchy + 40, ttl)
    for key, v in nodes.items():
        c, r, name, desc = v[:4]
        kind = v[4] if len(v) > 4 else 'svc'
        f, st = COL['purple' if kind == 'topic' else 'blue']
        if kind == 'topic':
            lab = f'<b>{esc(name)}</b><br><span style="font-size:9px">{esc(desc)}</span>'
            style = f'rounded=1;arcSize=50;whiteSpace=wrap;html=1;fillColor={f};strokeColor={st};fontFamily=Courier New;fontSize=10;'
            hh = 46
            yy = y0 + 34 + r * pitchy + (h - hh) / 2
        else:
            lab = f'<b>{esc(name)}</b><br><span style="font-size:9px">{esc(desc)}</span>'
            style = f'rounded=1;arcSize=8;whiteSpace=wrap;html=1;fillColor={f};strokeColor={st};fontSize=11;'
            hh = h
            yy = y0 + 34 + r * pitchy
            if len(desc) > 80: WARN.append(f'[{p.name}] comp {key}: desc long ({len(desc)})')
        p.vertex(x0 + 14 + c * pitchx, yy, w, hh, lab, style, key=key, node=True)
    for a, b, lbl, k in edges:
        p.edge(a, b, lbl, k)
    return y0 + rows * pitchy + 40 + 40
