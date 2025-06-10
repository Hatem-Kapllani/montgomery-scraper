1. Visit the website with URL: '''https://actweb.acttax.com/act_webdev/galveston/index.jsp'''

2. Click on the search box field with selector: '''#criteria'''

3. Write the search pattern in there starting from aaa%, aab%, aac% all the way to zzz%. We have previously combined 2 letters for the pattern and now we will do 3.

4. Click on the search button with selector: '''body > table:nth-child(2) > tbody > tr > td > table:nth-child(4) > tbody > tr > td > p:nth-child(5) > table:nth-child(6) > tbody > tr > td > center > form > table > tbody > tr:nth-child(5) > td:nth-child(2) > h3:nth-child(2) > input[type=submit]'''

5. From the results that will appear we will take the Account Number with selector: '''body > table:nth-child(2) > tbody > tr > td > table:nth-child(4) > tbody > tr > td > table:nth-child(7) > tbody > tr > td > table > tbody > tr:nth-child(1) > td:nth-child(1) > h3 > a'''

6. We will take Owner Name and Mailing Address which are combined into one field, with selector: '''body > table:nth-child(2) > tbody > tr > td > table:nth-child(4) > tbody > tr > td > table:nth-child(7) > tbody > tr > td > table > tbody > tr:nth-child(1) > td:nth-child(2) > h3'''. Then we will seperate the contents into 2 fields.

We will know where to seperate the content since the address starts with a number. For example: "AAA ACADEMY INC
2251 W FM 646 RD STE 135
DICKINSON, TX 77539''' , we know that the mailing address starts in the first number 2251 and includes everything from there . Everything before that is the Owner Name.

7. We will take the Property Address from the element with selector: '''body > table:nth-child(2) > tbody > tr > td > table:nth-child(4) > tbody > tr > td > table:nth-child(7) > tbody > tr > td > table > tbody > tr:nth-child(1) > td:nth-child(3) > h3'''. If this field is empty we should write UNKNOWN in the field in the output csv.

8. We will take Legal Description from the element with selector: '''body > table:nth-child(2) > tbody > tr > td > table:nth-child(4) > tbody > tr > td > table:nth-child(7) > tbody > tr > td > table > tbody > tr:nth-child(1) > td:nth-child(4) > h3'''

9. After finnishing with one search pattern we will go back on the original site and search for the next.


