"""Import provider/SMTP settings into existing demo.env without changing internal secrets.

Run on host: python3 configure_demo.py /private/path/provider.env
Does not contact providers, send emails or publish nginx changes.
"""
import argparse
from pathlib import Path
import shlex
from bootstrap import HERE

def read(path):
    result = {}
    for line in path.read_text(encoding='utf-8-sig').splitlines():
        if not line.strip() or line.lstrip().startswith('#') or '=' not in line:
            continue
        key,value = line.split('=',1)
        parsed = shlex.split(value, comments=True)
        result[key.strip()] = ' '.join(parsed)
    return result

if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('provider_file',type=Path)
    args=parser.parse_args()
    target=HERE/'demo.env'
    current=read(target)
    supplied=read(args.provider_file)
    allowed={'MGIMO_DEMO_DEEPSEEK_API_KEY', 'MGIMO_DEMO_MAIL_FROM',
             'MGIMO_DEMO_SMTP_HOST','MGIMO_DEMO_SMTP_PORT','MGIMO_DEMO_SMTP_USER',
             'MGIMO_DEMO_SMTP_PASSWORD','MGIMO_DEMO_SMTP_STARTTLS',
             'MGIMO_DEMO_FALLBACK_SMTP_HOST','MGIMO_DEMO_FALLBACK_SMTP_PORT',
             'MGIMO_DEMO_FALLBACK_SMTP_USER','MGIMO_DEMO_FALLBACK_SMTP_PASSWORD'}
    current.update({k:v for k,v in supplied.items() if k in allowed and v})
    missing=[k for k in ['MGIMO_DEMO_DEEPSEEK_API_KEY','MGIMO_DEMO_SMTP_HOST',
                         'MGIMO_DEMO_SMTP_USER','MGIMO_DEMO_SMTP_PASSWORD'] if not current.get(k)]
    if missing:
        raise SystemExit('Missing settings: '+', '.join(missing))
    if any("'" in v or '\n' in v or '\r' in v for v in current.values()):
        raise SystemExit('Unsupported quoting: configure dotenv manually without logging secrets.')
    temp=target.with_suffix('.env.new')
    with temp.open('x',encoding='utf-8') as f:
        temp.chmod(0o600)
        f.write(''.join(f"{k}='{v}'\n" for k,v in current.items()))
    temp.replace(target)
    print('Provider/SMTP settings installed. Recreate demo, test, then enable nginx routes.')
