1. Visit the website with URL: '''https://actweb.acttax.com/act_webdev/montgomery/index.jsp'''

2. Click on the search field with selector: '''#criteria'''

3. Write the search pattern there made up of 3 letters and % charachter. We need to move all the way from aaa% to aab% all the way to zzz%.

4. Click on the search button with selector: '''#content > table > tbody > tr:nth-child(1) > td > div:nth-child(3) > table > tbody > tr > td > center > form > table > tbody > tr:nth-child(3) > td:nth-child(2) > h3 > input[type=submit]'''

5. On the results that will appear we will take the Account Number with selector: '''#content > table > tbody > tr:nth-child(1) > td > table:nth-child(9) > tbody > tr > td > table > tbody > tr:nth-child(2) > td:nth-child(1) > h3 > a'''. We will save the Account Number with a preeceding ' so they can get as text instead of number.

6. We will take the Owner Name and Mailing Address from the element with selector: '''#content > table > tbody > tr:nth-child(1) > td > table:nth-child(9) > tbody > tr > td > table > tbody > tr:nth-child(2) > td:nth-child(2) > h3'''. After we get them we need to seperate them into their own fields. The mailing address starts with a number. From the number and back that is the mailing address, everything before that is the Owner Name.

7. We will get the Property Address from the element with selector: '''#content > table > tbody > tr:nth-child(1) > td > table:nth-child(9) > tbody > tr > td > table > tbody > tr:nth-child(2) > td:nth-child(3) > h3'''

8. We will take the Legal Description from the element with selector: '''#content > table > tbody > tr:nth-child(1) > td > table:nth-child(9) > tbody > tr > td > table > tbody > tr:nth-child(2) > td:nth-child(4) > h3'''

9. After we finnish we will go back to the original site and search the next search pattern.

10. If the search pattern doesnt have any results to show, it shows the element with selector: '''#content > table > tbody > tr:nth-child(1) > td > div:nth-child(3) > table > tbody > tr > td > center > form > table > tbody > tr:nth-child(1) > td:nth-child(2) > h3 > font > h6 > div''' and says "Your search found no records, please try again"