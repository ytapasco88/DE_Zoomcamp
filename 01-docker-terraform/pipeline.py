import sys
import pandas as pd

#Some fancy stuff with pandas

day = sys.argv[1]
print(f'Argumentos: {sys.argv}')
print(f'Version de pandas: {pd.__version__}')
print(f'job finished succesfully for day: {day}')