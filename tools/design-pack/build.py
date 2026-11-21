import sys
import xml.etree.ElementTree as ET
from lib import *
import diagrams1 as d1, diagrams2 as d2, content1 as c1, content2 as c2

pages = []
def P(i, name):
    p = Page(i, name)
    pages.append(p)
    return p

c1.p_cover(P(0, '00 Cover & Decisions'))
d1.p_l1(P(1, '01 L1 System Context'))
d1.p_access(P(2, '02 L2-A Access & Request Path'))
d1.p_ingest(P(3, '03 L2-B Ingestion & Normalization'))
d1.p_stream(P(4, '04 L2-C Stream & Batch Processing'))
d1.p_trading(P(5, '05 L2-D Quant, ML & Trading'))
c1.p_topics(P(6, '06 Kafka Topics & Conventions'))
c1.p_contracts(P(7, '07 Event & API Contracts'))
c1.p_data(P(8, '08 Data Architecture'))
d2.p_l3(P(9, '09 L3 Components'))
d2.p_seq_a(P(10, '10 Sequences A (real-time)'))
d2.p_seq_b(P(11, '11 Sequences B (research & ML)'))
p12 = P(12, '12 Deployment (Kubernetes)')
y = d2.p_deploy(p12, c2.NS)
c2.p_deploy_tables(p12, y)
p13 = P(13, '13 Observability, Security, NFR')
y = d2.p_obs(p13)
c2.p_cross_tables(p13, y)
c2.p_fail(P(14, '14 Failure Modes & Risks'))
c2.p_specs(P(15, '15 Service Specs 1-2'), 1)
c2.p_specs(P(16, '16 Service Specs 2-2'), 2)
c2.p_repo(P(17, '17 Repo Layout & Roadmap'))

xml = '<mxfile host="app.diagrams.net" agent="cip-generator" version="24.7.0" type="device">' + ''.join(p.xml() for p in pages) + '</mxfile>'
out = sys.argv[1] if len(sys.argv) > 1 else '../../docs/architecture/CIP-architecture-design-pack.drawio'
open(out, 'w', encoding='utf-8').write(xml)

# ---- validation ----
root = ET.fromstring(xml)
ok = True
for d in root.findall('diagram'):
    ids = [c.get('id') for c in d.iter('mxCell')]
    if len(ids) != len(set(ids)):
        print('DUP IDS in', d.get('name')); ok = False
    idset = set(ids)
    for c in d.iter('mxCell'):
        for a in ('source', 'target'):
            if c.get(a) and c.get(a) not in idset:
                print('BAD REF', d.get('name'), c.get('id')); ok = False
for p in pages:
    ns = p.nodes
    for i in range(len(ns)):
        for j in range(i + 1, len(ns)):
            a, b = ns[i], ns[j]
            if a[1] < b[1] + b[3] and b[1] < a[1] + a[3] and a[2] < b[2] + b[4] and b[2] < a[2] + a[4]:
                print('OVERLAP', p.name, a[0], b[0]); ok = False
print('WARN:', *WARN, sep='\n  ') if WARN else print('no size warnings')
print('pages', len(pages), 'bytes', len(xml), 'OK' if ok else 'ISSUES')
