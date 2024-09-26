import sys
import pandas as pd
import numpy as np


#Some fancy stuff with pandas

day = sys.argv[1]
print("=========================================")
print(f'Argumentos: {sys.argv}')
print(f'Version de pandas es la siguiente : {pd.__version__}')
print(f'Version de numpy es la siguiente: {np.__version__}')
print(f'job finished succesfully for day: {day}')
print("=========================================")