from Obelisk.bin.db_functions import db_connect_f, db_start_process, db_complete_process, get_process_parameters
from Obelisk.config.config_reader import ConfigReader
from Obelisk.mint.accounts import ACCOUNTS
from settings import mint_username, mint_password

from datetime import datetime, date, timedelta
import time
import mintapi
import os
import json

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


def json_date_to_datetime(dateraw: str) -> datetime:
    cy = date.today().year
    # cy = datetime.isocalendar(date.today())[0]
    try:
        newdate = datetime.strptime(dateraw + str(cy), '%b %d%Y')
    except ValueError:
        newdate = datetime.strptime(dateraw, '%m/%d/%y')
    return newdate


class Mint_API:
    def __init__(self, full_load=False):
        cr = ConfigReader()
        mint_config = cr.get_config(False, "projects", "Mint")
        self.ProcessType = mint_config.get("general", "ProcessType")
        self.ProcessName = mint_config.get("general", "ProcessName")
        params = json.loads(get_process_parameters(self.ProcessName))
        self.DaysToRun = int(params["DaysToRun"])
        self.username = mint_username
        self.password = mint_password
        self.full_load = full_load
        self.ProcessExecutionID = db_start_process(ProcessType=self.ProcessType,
                                                   ProcessName=self.ProcessName,
                                                   FullLoad=self.full_load)

    def extract(self):
        print('Starting Mint_API extraction')

        mint = mintapi.Mint(email=self.username, password=self.password, headless=True,
                            wait_for_sync=True, wait_for_sync_timeout=300, mfa_method='sms')

        task = 'mint.initiate_account_refresh()'
        try:
            mint.initiate_account_refresh()
            print('Accounts refreshed')
        except Exception as ex:
            print(f"Error running: {task}")
            print(ex)

        connection = db_connect_f()
        task = 'Querying Merchant Mapping'
        retry_flag = True
        retry_count = 0
        while retry_flag and retry_count < 5:
            try:
                MERCHANTS = list(connection.execute("SELECT LookupString, MerchantID FROM lookup.MerchantMapping"))
                # print(MERCHANTS)
                retry_flag = False
            except:
                print('Retry after 1 second')
                retry_count += 1
                time.sleep(1)
        if retry_count == 5:
            print(f"Error running: {task}")

        accounts = mint.get_accounts(True)

        for account in accounts:
            account_name = account['name']
            print(f"Accessing account: {account_name}")
            account_id = account['id']
            company = account['fiLoginDisplayName']
            account_balance = account['currentBalance']
            refreshed_on = account['lastUpdatedInDate'].strftime('%m/%d/%Y %H:%M:%S')
            is_active = account['accountSystemStatus']

            print(f"Merging data for account: {account_name}")
            task = 'EXEC etl.mint_Accounts_UI'
            retry_flag = True
            retry_count = 0
            query = open(f'{file_path}\\mint\\sprocs\\accounts.sql').read()
            #print(query.format(**vars()))
            while retry_flag and retry_count < 5:
                try:
                    connection.execute(query.format(**vars()))
                    connection.commit()
                    retry_flag = False
                except:
                    print('Retry after 1 second')
                    retry_count += 1
                    time.sleep(1)
            if retry_count == 5:
                print(f"Error running: {task}")

            if not self.full_load:
                start_date = (datetime.now() - timedelta(days=self.DaysToRun)).strftime('%m/%d/%y')
            else:
                start_date = (datetime.now() - timedelta(days=4000)).strftime('%m/%d/%y')

            print(f"Pulling data from account: {account_name}, starting at {start_date}")
            transactions = mint.get_transactions_json(id=account['id'], start_date=start_date)

            print(f"Inserting data for transactions from account: {account_name}")
            for transaction in transactions:
                transaction_id = transaction['id']
                account_name = transaction['account']
                account_id = ACCOUNTS[account_name]
                category_id = transaction['categoryId']
                category_name = transaction['category']
                transaction_date = json_date_to_datetime(transaction['odate'])
                is_debit = transaction['isDebit']
                if is_debit:
                    transaction_amount = -float(transaction['amount'].replace('$', '').replace(',', ''))
                else:
                    transaction_amount = float(transaction['amount'].replace('$', '').replace(',', ''))
                description = transaction['mmerchant'].replace("'", "").upper()
                for row in MERCHANTS:
                    a, b = tuple(row)
                    if description.find(a) != -1:
                        merchant_id = b
                        break
                    else:
                        merchant_id = 0

                task = 'EXEC etl.mint_Categories_I'
                retry_flag = True
                retry_count = 0
                query = open(f'{file_path}\\mint\\sprocs\\categories.sql').read()
                while retry_flag and retry_count < 5:
                    try:
                        connection.execute(query.format(**vars()))
                        connection.commit()
                        retry_flag = False
                    except:
                        print('Retry after 1 second')
                        retry_count = retry_count + 1
                        time.sleep(1)
                if retry_count == 5:
                    print(f"Error running: {task}")

                task = 'EXEC etl.mint_Transactions_UI'
                retry_flag = True
                retry_count = 0
                query = open(f'{file_path}\\mint\\sprocs\\transactions.sql').read()
                while retry_flag and retry_count < 5:
                    try:
                        connection.execute(query.format(**vars()))
                        connection.commit()
                        retry_flag = False
                    except:
                        print('Retry after 1 second')
                        retry_count += 1
                        time.sleep(1)
                if retry_count == 5:
                    print(f"Error running: {task}")

        task = 'mint.get_credit_score() & mint.get_net_worth()'
        retry_flag = True
        retry_count = 0
        while retry_flag and retry_count < 5:
            try:
                credit_score = mint.get_credit_score()
                net_worth = mint.get_net_worth()
                retry_flag = False
            except:
                print('Retry after 1 second')
                retry_count += 1
                time.sleep(1)
        if retry_count == 5:
            print(f"Error running: {task}")

        print("Inserting Overview Information")
        task = 'EXEC etl.mint_FinanceOverview_UI'
        retry_flag = True
        retry_count = 0
        query = open(f'{file_path}\\mint\\sprocs\\overview.sql').read()
        while retry_flag and retry_count < 5:
            try:
                connection.execute(query.format(**vars()))
                connection.commit()
                retry_flag = False
            except:
                print('Retry after 1 second')
                retry_count += 1
                time.sleep(1)
        if retry_count == 5:
            print(f"Error running: {task}")

        print('Completing Process')
        db_complete_process(self.ProcessExecutionID, self.ProcessType, self.ProcessName, self.full_load)
        connection.close()
        try:
            mint.close()
        except:
            print('mint.close() still broken')
