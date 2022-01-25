#!"c:\users\oanuf\google drive\programming\github\alpaca_trading\.venv\scripts\python.exe"
# EASY-INSTALL-ENTRY-SCRIPT: 'exchange-calendars==3.4','console_scripts','ecal'
__requires__ = 'exchange-calendars==3.4'
import re
import sys
from pkg_resources import load_entry_point

if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw?|\.exe)?$', '', sys.argv[0])
    sys.exit(
        load_entry_point('exchange-calendars==3.4', 'console_scripts', 'ecal')()
    )
