After the first scrape is finnished we will have a csv file in the codebase with the needed data called Montgomery2.csv from where we will take the records.

1. We will visit the same site as the first scraper.

2. We will click on the same search box as the first scraper. There we will write the Owner Name that we will take from that record in the csv.

3. Then we will click the same search button as in the first scraper. This will make that specific record show up in the site.

4. When the record shows up we need to click on the Account Number for which we have the selector in the first scraper. That will make the additional data that we need for that record, appear.

5. From there we will check the Property Address again with selector: '''#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(1) > h3:nth-child(3)'''. If the property address here is different from what we have in the csv, we need to update it to the new one we are checking. If it contains less or its the same, skip the new property address.

6. We will also take the Total Amount Due with selector: '''#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(1) > h3:nth-child(8)'''. If the amount here is $0.00 we need to write the data in the next steps as "Paid"

7. If the amount isnt $0.00 we will take Gross Value with selector: '''#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(2) > h3:nth-child(2)'''

8. We will take Land Value with selector: '''#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(2) > h3:nth-child(3)'''

9. We will take Improvement Value with selector: '''#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(2) > h3:nth-child(4)'''

10. We will take Capped Value with selector: '''#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(2) > h3:nth-child(5)'''

11. We will take Agricultural Value with selector: '''#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(2) > h3:nth-child(6)'''

12. We will take Exemptions with selector: '''#content > table > tbody > tr:nth-child(1) > td > table:nth-child(7) > tbody > tr:nth-child(2) > td:nth-child(2) > h3:nth-child(7)'''. If the value here is "None" we will convert it to $0.

13. We will also create a column called Total Taxable that we will calculate by comparing Gross value with Capped Value, taking the lesser from those 2 and substracting the Exemptions.
If one of the values of Gross Value or Capped Value is 0, we should automatically take the other one for the calculation. If the value of Exemptions is not numerical in the site  we should substract 0 from the calculation