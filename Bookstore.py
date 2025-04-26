import pandas as pd
import mysql.connector
 

# Database connection details
conn = mysql.connector.connect(
    host = 'localhost',  # Change this if your MySQL server is on a different host
    user = 'root',
    password = '',
    database = 'bookstore1'
)    



books_df=pd.read_sql('select * from books',conn)
authors_df=pd.read_sql('select * from author',conn)
publishers_df=pd.read_sql('select * from publisher',conn)
customers_df=pd.read_sql('select * from customer',conn)
purchases_df=pd.read_sql('select * from purchase_history',conn)


merged_df=purchases_df.merge(books_df,on='book_id') \
          .merge(authors_df,on='author_id') \
          .merge(publishers_df,on='publisher_id') \
          .merge(customers_df,on='customer_id')


top_books=merged_df.groupby('title')['quantity'].sum().sort_values(ascending=False).head(10)
print("Top 10 Best-selling books:\n", top_books)


merged_df['revenue']=merged_df['price']*merged_df['quantity']

author_revenue=merged_df.groupby('name_x')['revenue'].sum().sort_values(ascending=False)
print("\n Author Generating Highest Revenue:\n",author_revenue)


publisher_revenue=merged_df.groupby('publisher_name')['revenue'].sum()
print("\n Total Revenue by publisher:\n",publisher_revenue)


customer_purchases=merged_df.groupby('name_y')['quantity'].sum().sort_values(ascending=False)
print("\n Customer with highest number of purchases:\n",customer_purchases)


location_books=merged_df[merged_df['location_y']=='tokyo'].groupby('title')['quantity'].sum().sort_values(ascending=False).head(5)
print("\n Top 5 books in Tokyo:\n",location_books)

correlation=merged_df[['price','quantity']].corr()
print("\n correlation between price and purchase frequency:\n",correlation)

low_stock_books=books_df.sort_values('stock').head(5)
print("\n books with lowest stocks level:\n",low_stock_books[['title','stock']])

avg_stock_publisher=books_df.merge(publishers_df,on='publisher_id').groupby('publisher_name')['stock'].mean()
print("\n Average stock level by publisher:\n",avg_stock_publisher)

author_books_count=books_df.merge(authors_df,on='author_id').groupby('name')['book_id'].count().sort_values(ascending=False)
print("\n Author with most books:\n",author_books_count)

best_selling_books=merged_df.groupby('author_id')['quantity'].sum().reset_index()
top_authors=best_selling_books.merge(authors_df,on='author_id').groupby('nationality')['quantity'].sum().sort_values(ascending=False)
print("\n Most common nationality of best selling authors:\n",top_authors)


