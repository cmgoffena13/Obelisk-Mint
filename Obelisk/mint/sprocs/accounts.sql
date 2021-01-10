EXEC etl.mint_Accounts_UI @AccountID = {account_id},
                          @AccountName = '{account_name}',
                          @CompanyName = '{company}',
                          @CurrentBalance = {account_balance},
                          @LastRefreshedOn ='{refreshed_on}',
                          @Active = '{is_active}',
                          @ProcessExecutionID = {self.ProcessExecutionID}