from datetime import datetime
import pyodbc
import requests
import pandas as pd

startTime = datetime.now()

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=SV-GER-P-SQL7\P7_20636_01;'
                      'Database=nopcommerce;'
                      'Trusted_Connection=no;'
                      'UID=SAP;'
                      'PWD=S@P;')

#------------------------------ Insert Date Check Here
cursor = conn.cursor()
cursor.execute("Select Count(*)ValidCount FROM Product WHERE CONVERT(date, getdate()) = (SELECT CONVERT(VARCHAR(10), UpdatedOnUtc , 111))")
valCount = 0
for row in cursor:
    valCount = int(row.ValidCount)
if valCount >= 0:
    url = 'https://htpc20636p01.cloudiax.com:50000/b1s/v1/Login'
    payload = {'CompanyDB': 'A20636_BERNASOS_P01', 'Password': 'P@ssw0rd', 'UserName': 'SL'}
    headers = {'Content-Type': 'application/json', 'Accept': '*/*'}

    r = requests.post(url, json=payload, headers=headers, verify=False)
    print('Logged in')

    cursor.execute("SELECT COUNT(DISTINCT [Sku])SKU_No FROM [dbo].[Product] WHERE [Deleted] = 0 AND LEN([Sku]) = 9")
    dataCount = 0
    for row in cursor:
        dataCount = int(row.SKU_No)
    tup_list = []
    tup_list2 = []
    tup_list3 = []
    #------ Data count Modulus 20
    for i in range(0, dataCount, 20):
        print('i: ' + str(i))
        cursor.execute("SELECT DISTINCT [Sku] FROM [dbo].[Product] WHERE [Deleted] = 0 AND LEN([Sku]) = 9 ORDER BY [Sku] OFFSET " + str(i) + " ROWS FETCH NEXT 20 ROWS ONLY;")

        sqlCount = 0
        items = ""
        for row in cursor:
            if(items == ""):
                items += "ItemCode eq '" + row.Sku + "'"
            else:
                items += " or " + "ItemCode eq '" + row.Sku + "'"
            sqlCount += 1
        print('Retrieved ' + str(i + sqlCount) + ' items')
        #--------------------------------------------------------

        url2 = 'https://htpc20636p01.cloudiax.com:50000/b1s/v1/Items?$select=ItemCode,ItemName,QuantityOnStock,ItemPrices,ItemWarehouseInfoCollection,SalesVATGroup&$filter=' + items
        headers2 = {'Accept': '*/*', 'Cache-Control': 'no-cache', 'Content-Type': 'application/json'}
        y = requests.get(url=url2, headers=headers2, cookies=r.cookies, verify=False)
        response = y.json()
        #--------------------------------------------------------
        
        count = 0
        filteredWhses = ['1022','1021','1011','1131','1151','1052','1051']
        for item in response['value']:
            onHand = 0
            itemCode = item['ItemCode']
            priceList1 = item['ItemPrices'][0]['Price']
            priceList2 = item['ItemPrices'][1]['Price']
            priceList5 = item['ItemPrices'][4]['Price']
            instock = int(item['QuantityOnStock'])
            salesGroup = item['SalesVATGroup']
            disableButt = 0
            if instock <= 0 or priceList1 == 0:
                disableButt = 1
            if salesGroup == 'SMVAT':
                priceList1 = priceList1 * 1.14
                priceList2 = priceList2 * 1.14
                priceList5 = priceList5 * 1.14
            for whse in item['ItemWarehouseInfoCollection']:
                if whse['WarehouseCode'] in filteredWhses:
                    onHand = onHand + whse['InStock']
            tup = (priceList1, onHand, disableButt, itemCode)
            tup_list.append(tup)
            tup2 = (priceList5, itemCode)
            tup_list2.append(tup2)
            tup3 = (priceList2, itemCode)
            tup_list3.append(tup3)
            count += 1
        print('Appended ' + str(count) + ' items')

    print('Tuple List length is: ' + str(len(tup_list)))
    df = pd.DataFrame(tup_list)
    df2 = pd.DataFrame(tup_list2)
    df3 = pd.DataFrame(tup_list3)
    print(df)
    cursor = conn.cursor()
    #print(df.values.tolist())
    #print(df2.values.tolist())
    #print(df3.values.tolist())
    print('Step 1 Update...')    
    cursor.executemany("UPDATE [dbo].[Product] SET [Price] = ?, [StockQuantity] = ?, [DisableBuyButton] = ?, [UpdatedOnUtc] = CURRENT_TIMESTAMP WHERE [Sku] = ?;", df.values.tolist())    
    print('Step 2 Update...')
    cursor.executemany("UPDATE [dbo].[TierPrice] SET [Price] = ?, [StartDateTimeUtc] = CURRENT_TIMESTAMP FROM [dbo].[TierPrice] T INNER JOIN [dbo].[Product] P ON T.[ProductId] = P.[Id] WHERE P.[Sku] = ? AND CustomerRoleId = 7;",df2.values.tolist())
    print('Step 3 Update...')
    cursor.executemany("UPDATE [dbo].[TierPrice] SET [Price] = ?, [StartDateTimeUtc] = CURRENT_TIMESTAMP FROM [dbo].[TierPrice] T INNER JOIN [dbo].[Product] P ON T.[ProductId] = P.[Id] WHERE P.[Sku] = ? AND CustomerRoleId = 8;",df3.values.tolist())
    print('Comitting Updates Please Wait...')
    conn.commit() 
    cursor.close()
    conn.close()
    print('Update Complete. This script was executed in : ' + str(datetime.now() - startTime))
