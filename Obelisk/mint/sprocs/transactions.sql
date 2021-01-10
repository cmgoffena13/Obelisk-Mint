EXEC etl.mint_Transactions_UI @TransactionID = {transaction_id},
                              @AccountID = {account_id},
                              @DateOfTransaction = '{transaction_date}',
                              @TransactionAmount = {transaction_amount},
                              @TransactionDescription = '{description}',
                              @MerchantID = {merchant_id},
                              @CategoryID = {category_id},
                              @ProcessExecutionID = {self.ProcessExecutionID}
