# Runs entire Obelisk ETL process
from Obelisk.mint.mint_extract import Mint_API
import os

file_path = os.path.abspath(os.path.dirname(__file__))
full_load = False

if __name__ == '__main__':

    os.system(f"{file_path}\\venv\\Scripts\\activate")

    print('Starting Obelisk ETL')
    print('Starting Obelisk Extracts')
    Mint = Mint_API(full_load=full_load)
    Mint.extract()
    print('Completed Obelisk Extracts')
    print('Starting Obelisk Transforms')

    print('Completed Obelisk Transforms')
    print('Starting Obelisk Loads')

    print('Completed Obelisk Loads')
    print('Completed Obelisk ETL')

    os.system("deactivate")