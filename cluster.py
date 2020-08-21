from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra.query import SimpleStatement
import datetime
import cassandra.util
import uuid
import time
import sys
import Screen

cluster = Cluster(['127.0.0.1'], port = 9042)

session = cluster.connect()

result = session.execute('use killrvideo;')

print('keyspace connected');


def users_register(emailid, password):
    userid = str(uuid.uuid1())
    
    session.execute("INSERT INTO users_register(emailid, password, userid) VALUES('"+emailid+"','"+password+"',"+userid+")")
    session.execute("INSERT INTO users_credentials(userid, password) VALUES("+userid+",'"+password+"')")
    session.execute("INSERT INTO user_details(userid,email) values("+userid+",'"+emailid+"')")
    
    time.sleep(2)
    return emailid,userid
    
def users_credentials(userid):
    result = session.execute(f"SELECT userid FROM users_credentials WHERE userid ={userid}")
    ids=''
    for val in result:
        ids = str(val[0])
    if(ids == str(userid)):
        time.sleep(2)
        print('Successfully Authenticated :)')
        return str(userid),ids
    else:
        print('Unable to autheticate :( Make sure you have successfully registered')
        return str(userid),ids
    
def latest_products():
    sort_by_year = int(input('sortby_year = '))
    print('{0:40} {1:20} {2:40} {3:20} {4:5}'.format('PRODUCTID','TITLE','ADDED_BY','ADDED_AT','YEAR'))
    for val in session.execute(f"SELECT productid,title,userid,added_at,year FROM latest_products WHERE year = {sort_by_year}"):
          print('{0:5} {1:20} {2:40} {3:20} {4:5}'.format(str(val[0]), val[1], str(val[2]), str(val[3]), val[4]))

def product_details(productid):
    print('*****PRODUCT DETAILS******')
    result = session.execute(f"SELECT productid,title,description,cost,discount,added_at,added_by,tags,isavailable,stockleft FROM product_details WHERE productid = {productid}")
    for val in result:
        #print('{0:5} {1:15} {2:40} {3:40} {4:40} {5:40 {6:20}'.format(val[0], val[1], val[2], val[3], val[4], str(val[5]), str(val[6]), val[7]))
        print('Productid = ',val[0])
        print('Product Title = ',val[1])
        print('Product Description = ',val[2])
        print('Product Cost = ',val[3])
        print('Product Discount = ',val[4])
        print('Product added at = ',str(val[5]))
        print('Product added by = ',str(val[6]))
        print('Tags = ',val[7])
        print('Is the Product Still available? ',str(val[8]))
        print('Number of stocks left= ',val[9])

def update_product_details(productid):
    cost = str(float(input('Update the cost of this product = ')))
    discount = str(float(input('Update the discount for this product = ')))
    stockleft = str(input('Update the number of stocks available = '))
    session.execute("UPDATE product_details SET cost="+cost+", discount="+discount+", stockleft="+stockleft+" WHERE productid ="+productid)
    print('your product has been updated successfully :)')

def update_products_purchased(userid, title):
    session.execute("UPDATE user_details SET products_purchased+={'"+str(title)+"'} WHERE userid="+userid)


def user_details(userid):
    print('*****USER DETAILS******')
    for val in session.execute(f"SELECT userid,firstname,lastname,email,address,products_purchased FROM user_details WHERE userid ={userid}"):
        print('Userid = ',str(val[0]))
        print('FirstName = ',str(val[1]),', LastName = ',str(val[2]))
        print('Email Id = ',str(val[3]))
        print('Full Address = ',str(val[4]))
        print('Products purchased so far = ',str(val[5]))

def update_user_name(userid):
    firstname = str(input('Update your FirstName = '))
    lastname = str(input('Update your LastName = '))
    session.execute("UPDATE user_details SET firstname='"+firstname+"', lastname='"+lastname+"' WHERE userid ="+userid)
    print('your Name has been updated successfully :)')

def update_user_address(userid):
    doorid = str(input('Update your Door Id = '))
    street = str(input('Update your street = '))
    city = str(input('Update your city = '))
    country = str(input('Update your country = '))
    pincode = str(int(input('Update your Pincode = ')))
    session.execute("UPDATE user_details SET address={doorid:'"+doorid+"', street:'"+street+"', city:'"+city+"', country:'"+country+"', pincode:"+pincode+"} WHERE userid="+userid)
    print('your New Address has been updated successfully :)')

def add_product(userid):
    title = str(input('Enter the Product name = '))
    description = str(input('Enter the Product Description = '))
    cost = str(float(input('Enter the Cost of the product = ')))
    discount = str(float(input('Enter the Discount of the product = ')))
    year_added = str(int(input('Enter the year = ')))
    isavailable = str(bool(input('Is the product available? Reply True or False..')))
    stockleft = str(int(input('Enter Number of Stocks Left = ')))

    i = 0
    tags={}
    while(True):
        choice = int(input('Do you want to add tags to the product? Enter 1 for yes & 0 for No..'))
        if(choice == 1):
            print('Add a Tag below !!')
            tags[i]= input('')
            i=i+1
        elif(choice == 0):
            break;

    date = datetime.datetime.now()
    productid = str(cassandra.util.uuid_from_time(date))
    added_at = str(date.date())
    
    session.execute("INSERT INTO add_product(userid,added_at,productid,title,description,cost,discount,isavailable,stockleft) VALUES("+userid+",'"+added_at+"',"+productid+",'"+title+"','"+description+"',"+cost+","+discount+","+isavailable+","+stockleft+")")
    session.execute("Insert into product_details(productid,added_at,added_by,cost,description,discount,title,isavailable,stockleft) values("+productid+",'"+added_at+"',"+userid+","+cost+",'"+description+"',"+discount+",'"+title+"',"+isavailable+","+stockleft+")")
    session.execute("Insert into latest_products(year,added_at,productid,title,userid) values("+year_added+",'"+added_at+"',"+productid+",'"+title+"',"+userid+")")

    k = 0
    while(k<i):
        session.execute("update add_product set tags+={'"+tags.get(k)+"'} where userid="+userid+" AND added_at='"+added_at+"' AND title='"+title+"' AND productid="+productid+"")
        session.execute("update product_details set tags+={'"+tags.get(k)+"'} where productid="+productid+"")
        k+=1
        
    print('Your product "'+title+'" has been successfully added for the sale !!')
    print(productid+' is your productid')
    

def update_product(userid,productid):
    result = session.execute(f"SELECT added_at,title from product_details WHERE productid={productid}")
    for val in result:
        added_at = str(val[0])
        title = str(val[1])
    cost = str(float(input('Update the cost of the product = ')))
    discount = str(float(input('Update the Discount of the product = ')))
    isavailable = str(bool(input('Is the product still available? Reply True or False..')))
    if(isavailable == 'True'):
        stockleft = str(int(input('Update the stock left = ')))
    else:
        stockleft = str(0)

    session.execute("UPDATE add_product SET cost ="+cost+", discount="+discount+", isavailable="+isavailable+", stockleft="+stockleft+" WHERE userid="+userid+" AND added_at='"+added_at+"' AND title='"+title+"' AND productid="+productid+"")
    session.execute("UPDATE product_details SET cost="+cost+", discount="+discount+", isavailable="+isavailable+", stockleft="+stockleft+" WHERE productid="+productid+"")
    print('Successfully updated !!!')

def buy_product(userid,productid):
    stock = int(input('Enter number of stocks needed = '))
    stocks = str(stock)
    acc_type = str(input('Enter yout Account type {CC ~ Credit Card / DC ~ Debit Card / IB ~ Internet Banking} = '))
    date = datetime.datetime.now()
    transactionid = str(cassandra.util.uuid_from_time(date))
    transaction_at = str(date.date())
    acc = str(input('Enter your Card / Bank Account number = '))
    pin = str(input('Enter your Pin number = '))
    acc_details = {'acc_no': acc, 'pin_no':pin}
    session.execute("Insert into account_type(acc_type,transactionid,transaction_at,userid,acc_details) values('"+acc_type+"',"+transactionid+",'"+transaction_at+"',"+userid+",{acc_no:'"+acc+"',pin_no:'"+pin+"'})")
 
    print('Linking to your Bank Account....')
    time.sleep(1)
    
    result = session.execute(f"SELECT cost,discount,isavailable,stockleft from product_details WHERE productid={productid}")
    for val in result:
        isavailable = str(val[2])
        stockleft = val[3]
        cost = val[0]
        discount = val[1]
    if(isavailable == 'True'):
        if(stock<=stockleft):
            totalcost = str((cost/100)*discount*stock)
            stockleft = str(stockleft-stock)
            cost = str(val[0])
            discount = str(val[1])
            result = session.execute(f"SELECT added_at,title from product_details WHERE productid={productid}")
            for val in result:
                added_at = str(val[0])
                title = str(val[1])
            print('The cost of the\total Item is Rs. '+totalcost+"\n\n")
            print('Transaction in Progress......')
            time.sleep(1)
            session.execute("update add_product set stockleft="+stockleft+" where userid="+userid+" AND added_at='"+added_at+"' AND title='"+title+"' AND productid="+productid+"")
            session.execute("update product_details set stockleft="+stockleft+" where productid="+productid+"")
            session.execute("Insert into buy_product(productid,transactionid,transaction_at,userid,cost,stocks,title) values("+productid+","+transactionid+",'"+transaction_at+"',"+userid+","+totalcost+","+stocks+",'"+title+"')")
            titles = {}
            titles[0] = title
            session.execute("update user_details set products_purchased+={'"+str(titles.get(0))+"'} where userid="+userid+"")
            print('Howzaat ! your purchase is successful and the produuct is on the way !!')
        else:
            print("we don't have sufficient products ! please do check here frequently for uploaded stocks")
    elif(isavailable == 'False'):
        print('No Stock left....')
        stockleft = 0
        session.execute("update add_product set isavailable="+isavailable+", stockleft="+stockleft+" where productid="+productid)
        session.execute("update product_details set isavailable="+isavailable+", stockleft="+stockleft+" where productid="+productid)

def delete_server(emailid,userid):
    if(emailid == 'chandrasekar.b03@gmail.com'):
        session.execute("TRUNCATE TABLE users_register")
        session.execute("TRUNCATE TABLE users_credentials")
        session.execute("TRUNCATE TABLE add_product")
        print("AS a request from Admin, We have flushed all data due to server problem !!!")
    
    else:
        print('Check & confirm your Emailid again please...!!!')


def add_comment(userid,productid):
    comments = {}
    comment = str(input('Enter a comment here = '))
    comments[0] = str(comment)
    date = datetime.datetime.now()
    commentid = str(cassandra.util.uuid_from_time(date))
    commented_at = str(date.date())
    session.execute("INSERT into comments(userid,commentid,commented_at,comment) values("+userid+","+commentid+",'"+commented_at+"','"+str(comments.get(0))+"')")
    session.execute("update product_details set comments+={'"+str(comments.get(0))+"'} where productid="+productid+"")
    session.execute("update user_details set comments+={'"+str(comments.get(0))+"'} where userid="+userid+"")

def latest_comments_by_user(userid):
    result = session.execute(f"select userid,commented_at,comment,commentid from comments where userid={userid}")
    for val in result:
        print('{0:40} {1:15} {2:60} {3:40}'.format(str(val[0]), str(val[1]), str(val[2]), str(val[3])))

def latest_comments_by_product(productid):
    result = session.execute(f"select comments from product_details where productid={productid}")
    for val in result:
        print(" "+str(val))

def delete_account(emailid,userid):
    try:
        session.execute("delete from users_register where emailid='"+emailid+" AND userid="+userid+"")
        session.execute("delete from users_credentials where userid="+userid+"")
        session.execute("delete from add_product where emailid='"+emailid+" AND userid="+userid+"")
        print("Account deleted!! We are so sorry that you had a bad expereince in our application. We would like to hear from you soon.")
        
    except:
        print("sorry !! you are not recognized")
def sales(userid):
    print('Dear user, to Navigate to pages follow the below instructions carefully.')
    print('To view Latest products, Click 4\n'
            +'To view Product Details, Click 5\n'
            +'To Update your Profile, Click 6\n'
            +'To view your Profile, Click 7\n'
            +'To Add Product, Click 8\n'
            +'To Update your Product, Click 9\n'
            +'To Buy your Product, Click 10\n'
            +'To Delete all Accounts and flush (Only admins for backing up the data), Click 11\n'          
            +'To Comment on your Product, Click 12\n'
          +'To View all your comments, Click 13\n'
          +'To View Comments on a Product, Click 14\n'
          +'To Quit Application, Click 15\n')

    choice = int(input('Enter your Choice = '))
    if(choice == 4):    #view latest products
        latest_products()
    elif(choice == 5):  #View a specific Product Details
        productid = input('Enter productid = ')
        product_details(productid)
    elif(choice == 6):  #Update your Profile
        print('Click 1 to update your Full Name')
        print('Click 2 to update your Address')
        choice = int(input('Enter your choice  = '))
        if(choice == 1):
            update_user_name(userid)
        elif(choice == 2):
            update_user_address(userid)
    elif(choice == 7):  #View your profile
        user_details(userid)
    elif(choice == 8):  #Add your Product for sale
        add_product(userid)
    elif(choice == 9):  #Update your product for sale
        productid = input('Enter productid to update the added product = ')
        update_product(userid,str(productid))
    elif(choice == 10):  #Buy a Product
        productid = input('Enter productid to buy the product = ')
        buy_product(userid,str(productid))
    elif(choice == 11):  #Delete all Accounts and flush (Only admins for backing up the data)
        emailid = input('Confirm by Entering your Emailid again = ')
        delete_server(emailid,userid)
    elif(choice == 12):     # Add a Comment to a product
        productid = input('Enter productid to Comment on the product = ')
        add_comment(userid,str(productid))
    elif(choice == 13):     # View latest Comments by an user
        latest_comments_by_user(userid)
    elif(choice == 14):     #view latest comments by a product
        productid = input('Enter productid to Comment on the product = ')
        latest_comments_by_product(str(productid))
    elif(choice == 15):  #Quit from this application
        sys.exit()


def loop():
    print('**********************Electronics Shopping plaza*********************')

    print('Click 1 to view to view Latest Products')
    print('Else, Click 0 to Register or Authenticate.')

    choice = int(input('Enter your choice = '))
    if(choice == 1):
        latest_products()
        uid=''
    elif(choice == 0):
        print('To Register Click  2.')
        print('Else to Authenticate Click 3')
        choice = int(input('Enter your choice = '))
        if(choice == 2):
            print('**************User Registration*****************')
            emailid = input('Enter your Email= ')
            password = input('Enter your password= ')
            emailid, userid = users_register(emailid,password)
            user = str(userid)
            print(user,' has been registered for the email id ',emailid)
            userid,ids = users_credentials(user)
            uid = user
        elif(choice == 3):
            user = input('Enter your user Id = ')
            print('Authenticating your Account.....')
            userid,ids = users_credentials(user)
            uid = str(userid)

    if(uid == ids):
        while(True):
            time.sleep(2)
            sales(str(userid))
    else:
        print('This seems that you may have not Registered or Authenticated ! Please Try Again :(')
        choice = int(input('To go back to registeration or authentication,please Click 3... Else click 0 to quit...'))
        if(choice == 3):
            Screen.clear()
            loop()
        elif(choice == 0):
            sys.exit()
            
loop()
session.shutdown()
cluster.shutdown()








#print('{0:12} {1:40} {2:5}'.format('Tag', 'ID', 'Title'))
#for val in session.execute("select * from videos_by_tag"):
#    print('{0:12} {1:40} {2:5}'.format(val[0], str(val[2]), val[3]))

#session.execute("INSERT INTO videos_by_tag(tag, video_id, added_date, title) values('Cloud Native', uuid(), toTimestamp(now()),'Cloud Native Workshop Series');")

#print('.......................................')
#print('..........')

#for val in session.execute("select * from videos_by_tag"):
#    print('{0:12} {1:40} {2:5}'.format(val[0], str(val[2]), val[3]))

    
#session.shutdown()
#cluster.shutdown()


