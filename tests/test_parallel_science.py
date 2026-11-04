"""Exercise actual streaming science in an isolated subprocess/config namespace."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = r"""
import json
from pathlib import Path
import sys
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from ipid_analysis import config
root=Path(sys.argv[1])
config.RAW_DATA_DIR=root/'raw'
config.PROCESSED_DATA_DIR=root/'processed'
config.FIGURES_DIR=root/'figures'
from ipid_analysis import parallel as p
from ipid_analysis.artifacts import file_digest
from ipid_analysis.manifest import IpidMeasurement
from ipid_analysis.scheduler import Task
from tests.test_continental import FakeClient
client=FakeClient()
campaign='s3://bucket/workflow/continental/round'
flow=campaign+'/sites/eu/protocols/icmp'
raw='s3://bucket/raw/continental/round/eu'
client.json(campaign+'/campaign.json',dict(version=1,campaign_id='round',sites=list(p.SITES),protocols=list(p.PROTOCOLS),raw_root='s3://bucket/raw'))
client.json(campaign+'/parallel.json',dict(version=1,campaign_id='round',mode='parallel-protocols'))
zid='icmp_2026-11-01_01-00-00'
for variant,second,empty in [('rt-base',1,False),('repeat',2,True)]:
    mid=f'icmp_2026-11-01_01-00-{second:02d}'
    n=100 if empty else 16
    source=root/f'{second}.pq'
    columns={k:pa.array(v,type=pa.string()) for k,v in dict(IP_ADDR=[] if empty else ['198.51.100.1'],IPID_SEQUENCE=[] if empty else [','.join(['99']*n)],SEND_TIMESTAMP_SEQUENCE=[] if empty else [','.join(str(i*20000) for i in range(n))]).items()}
    pq.write_table(pa.table(columns),source)
    snapshot=dict(connection_count=4,requests_per_connection=n//4,request_ip_ids=[1,2],measurement_mode='fixed-interval' if empty else 'rt-based',zmap=zid,tcp=dict(establish_connection=False),fixed_interval=dict(minimum_reply_rate=0.8,request_interval='20ms'))
    snap=root/f'{second}.yaml'
    snap.write_text(yaml.safe_dump(snapshot))
    evidence=lambda path:dict(size_bytes=path.stat().st_size,sha256=file_digest(path))
    for name,path in [('ipid.pq',source),('ipid.snapshot.yaml',snap)]:
        client.values[f'{raw}/ipid/{mid}/{name}']=path.read_bytes()
    client.json(f'{raw}/ipid/{mid}/measurement-complete.json',dict(version=1,measurement_id=mid,files={'ipid.pq':evidence(source),'ipid.snapshot.yaml':evidence(snap)}))
    target=root/f'target-{second}.pq'
    pq.write_table(pa.table(dict(IP_ADDR=pa.array([] if empty else ['198.51.100.1'],type=pa.string()),REPLY_TYPE=pa.array([] if empty else [''],type=pa.string()))),target)
    target_uri=raw+'/ipid/icmp_2026-11-01_00-00-01/random-reproducibility-targets.pq' if empty else raw+'/zmap/'+zid+'/zmap.pq'
    client.values[target_uri]=target.read_bytes()
    uri=f'{flow}/measurement-jobs/{mid}/request.json'
    client.json(uri,dict(version=1,campaign_id='round',site='eu',protocol='icmp',measurement_id=mid,zmap_id=zid,variant=variant,raw_prefix=raw+'/ipid',zmap_prefix=raw+'/zmap',target=dict(uri=target_uri,**evidence(target))))
    p.measurement(client,Task('parallel-measurement',uri,flow,'continental/round/eu',10),root/f'work-{second}',100,1)
    assert client.exists(uri.replace('request.json','done.json'))
    m=IpidMeasurement('icmp','no-connection','fixed-interval' if empty else 'rt-based','mass' if empty else 'base',mid,mid)
    for kind,extension,folder in [('coverage','json',config.PROCESSED_DATA_DIR),('strategies','pq',config.PROCESSED_DATA_DIR),('increments','pq',config.PROCESSED_DATA_DIR),('probing-intervals','pq',config.PROCESSED_DATA_DIR),('strategies','pdf',config.FIGURES_DIR),('increments','pdf',config.FIGURES_DIR),('probing-intervals','pdf',config.FIGURES_DIR)]:
        assert m.artifact_path(folder,kind,extension).is_file(),(kind,mid)
    assert json.loads(m.artifact_path(config.PROCESSED_DATA_DIR,'coverage','json').read_text())['zmap_ip_count']==(0 if empty else 1)
    # A resumed event reuses artifacts; canonical ZMap output paths stay untouched.
    assert not (config.PROCESSED_DATA_DIR/zid).exists()
    from unittest.mock import patch
    from tests.test_plot_os_group_strategy import OSGroupStrategyPlotTest
    shared=root/'shared-os'
    OSGroupStrategyPlotTest._write_os(shared/'os.pq',['198.51.100.1'],['resolved'],['linux'])
    with patch.object(p,'verify_shared_os',return_value=(shared,dict(version=1))):
        os_task=Task('parallel-measurement-os',uri,flow,'continental/round/eu',10)
        p.measurement_os(client,os_task,root/f'work-os-{second}',1)
        os_done=client.value(p.success_uri(os_task))
        assert os_done['measurement_id']==mid
        if empty:
            assert os_done['status']=='not_applicable_empty_population'
        else:
            assert m.artifact_path(config.FIGURES_DIR,'operating-system-group-by-strategy','pdf').is_file()

# Run strict canonical science through the CLI while shared OS is absent.
from typer.testing import CliRunner
from ipid_analysis import postprocess
import shutil
base='icmp_2026-11-01_01-00-21'
mass='icmp_2026-11-01_01-00-11'
fixed='icmp_2026-11-01_01-00-12'
zdir=config.RAW_DATA_DIR/'zmap'/zid
zdir.mkdir(parents=True)
for filename in ('zmap.pq','zmap-fixed-base-sample.pq'):
    shutil.copy(root/'target-1.pq',zdir/filename)
base_dir=config.RAW_DATA_DIR/'ipid'/base
base_dir.mkdir(parents=True)
random_base=[33375,55746,60367,41743,55073,33497,48632,17680,59576,20173,15785,2685,61469,4930,10166,1083]
pq.write_table(pa.table(dict(IP_ADDR=['198.51.100.1'],IPID_SEQUENCE=[','.join(map(str,random_base))],SEND_TIMESTAMP_SEQUENCE=[','.join(str(i*20000) for i in range(16))])),base_dir/'ipid.pq')
(base_dir/'ipid.snapshot.yaml').write_text(yaml.safe_dump(dict(snapshot,requests_per_connection=4,measurement_mode='rt-based')))
shutil.copy(root/'target-1.pq',base_dir/'zmap_unclassified.pq')
for mid,n in ((mass,100),(fixed,16)):
    folder=config.RAW_DATA_DIR/'ipid'/mid
    folder.mkdir(parents=True)
    pq.write_table(pa.table(dict(IP_ADDR=['198.51.100.1'],IPID_SEQUENCE=[','.join(['99']*n)],SEND_TIMESTAMP_SEQUENCE=[','.join(str(i*20000) for i in range(n))])),folder/'ipid.pq')
    value=dict(snapshot,requests_per_connection=n//4,measurement_mode='fixed-interval')
    (folder/'ipid.snapshot.yaml').write_text(yaml.safe_dump(value))
manifest=root/'manifest.json'
manifest.write_text(json.dumps(dict(icmp=dict(zmap=zid,os='shared-os',ipid={'no-connection':{'rt-based':{'base':base},'fixed-interval':{'mass':mass,'base':fixed}}}))))
status=root/'core-status.json'
with patch.object(postprocess,'prepare_itdk',side_effect=AssertionError('external preparation in core')),patch.object(postprocess,'prepare_ripe_atlas',side_effect=AssertionError('external preparation in core')),patch.object(postprocess,'render_os_strategy_plot',side_effect=AssertionError('OS dependency in core')):
    result=CliRunner().invoke(postprocess.app,[str(manifest),'--phase','core','--strict','--threads','1','--status-file',str(status)])
    assert result.exit_code==0,(result.output,result.exception)
report=json.loads(status.read_text())
assert report['phase']=='core' and report['required_complete'] is True,report
assert report['optional']['itdk']=='skipped' and report['optional']['ripe']=='skipped'
"""


class ParallelScienceTests(unittest.TestCase):
    def test_real_streaming_reports_and_empty_frozen_repeat(self):
        with tempfile.TemporaryDirectory() as directory:
            env = dict(
                os.environ,
                MPLBACKEND="Agg",
                IPID_ANALYSIS_STORAGE_MODE="local",
                IPID_ANALYSIS_TMP_DIR=directory + "/scratch",
                IPID_ANALYSIS_CONTROL_ROOT=directory + "/control",
            )
            # WSL1 omits MemAvailable; the external local test launcher handles
            # that host limitation. CI uses the ordinary runtime unchanged.
            result = subprocess.run(
                [sys.executable, "-c", SCRIPT, directory],
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=120,
            )
            self.assertEqual(result.returncode, 0, result.stdout[-12000:])


if __name__ == "__main__":
    unittest.main()
