import psycopg2

class poatek_database:

    def __init__(self):
        self.dbconnection = None

    def connection(self):
        # connecting to the database
        self.dbconnection = psycopg2.connect(host="poatek-internship-program.cs14eynyg5uq.us-east-2.rds.amazonaws.com",
                              database="internship", user="intern", password="poatek123", port="5432")

    def end_connection(self):
        self.dbconnection.close()

    def query21(self):
        # creates cursors
        first_cursor = self.dbconnection.cursor()
        second_cursor = self.dbconnection.cursor()

        # execute query
        first_cursor.execute("select * from sales.sales_order")
        second_cursor.execute("select * from sales.customers")

        # fetch information
        rows = first_cursor.fetchall()
        customer_rows = second_cursor.fetchall()

        customer_idlist = []
        customer_pricelist = []
        for row in rows:
            if row[9] in customer_idlist:       #collect all the prices and put in list
                customer_pricelist[customer_idlist.index(row[9])] += row[12]*row[13]
            else:                               #get all customer ids and prices and append to lists
                customer_idlist.append(row[9])
                customer_pricelist.append(row[12]*row[13])

        top_five = []
        for i in range(0, 5):                   #see which ids have the biggest prices
            biggest_num = 0
            for price in customer_pricelist:
                if biggest_num < price:
                    biggest_num = price

            ind = customer_pricelist.index(biggest_num)
            top_five.append(customer_idlist[ind])
            customer_pricelist.pop(ind)
            customer_idlist.pop(ind)

        #prints top five customers
        print(f"The five customers that bought the most in USD are: {customer_rows[top_five[0]-1][1]}, {customer_rows[top_five[1]-1][1]}, {customer_rows[top_five[2]-1][1]}, {customer_rows[top_five[3]-1][1]}, {customer_rows[top_five[4]-1][1]}.")

        # closes the cursors
        first_cursor.close()
        second_cursor.close()

    def query22(self):
        # creates cursors
        first_cursor = self.dbconnection.cursor()
        second_cursor = self.dbconnection.cursor()

        # execute query
        first_cursor.execute("select * from sales.sales_order")
        second_cursor.execute("select * from sales.store_locations")

        # fetch information
        rows = first_cursor.fetchall()
        store_rows = second_cursor.fetchall()

        store_idlist = []
        order_quantitylist = []
        for row in rows:
            if row[10] in store_idlist:     #collect all order quantities and put in a list
                order_quantitylist[store_idlist.index(row[10])] += row[12]
            else:                           #get all store ids and order quantities and append to lists
                store_idlist.append(row[10])
                order_quantitylist.append(row[12])

        state_list = []
        state_quantitylist = []
        for store_id in store_idlist:       #add quantities to each individual state
            if store_rows[store_id-1][4] in state_list:
                state_quantitylist[state_list.index(store_rows[store_id-1][4])] += order_quantitylist[store_idlist.index(store_id)]
            else:
                state_list.append(store_rows[store_id-1][4])
                state_quantitylist.append(order_quantitylist[store_idlist.index(store_id)])

        biggest_num = 0
        for num in state_quantitylist:
            if biggest_num < num:
                biggest_num = num

        #prints the answer
        print(f"The state that has sold the most in terms of units is '{state_list[state_quantitylist.index(biggest_num)]}'.")

        # closes the cursors
        first_cursor.close()
        second_cursor.close()

    def query23(self):
        # creates cursors
        first_cursor = self.dbconnection.cursor()
        second_cursor = self.dbconnection.cursor()

        # execute query
        first_cursor.execute("select * from sales.sales_order")
        second_cursor.execute("select * from sales.store_locations")

        # fetch information
        rows = first_cursor.fetchall()
        store_rows = second_cursor.fetchall()

        store_idlist = []
        order_quantitylist = []
        for row in rows:
            if row[10] in store_idlist:         #collect all order quantities and put in a list
                order_quantitylist[store_idlist.index(row[10])] += row[12]
            else:                               #get all store ids and order quantities and append to lists
                store_idlist.append(row[10])
                order_quantitylist.append(row[12])

        indiana_city_list = []
        indiana_quantity_list = []
        for store_id in store_idlist:
            if store_rows[store_id-1][4] == 'Indiana' and store_rows[store_id-1][1] not in indiana_city_list:   #If the city is in 'Indiana' and the city is not on the list
                indiana_city_list.append(store_rows[store_id-1][1])
                indiana_quantity_list.append(order_quantitylist[store_idlist.index(store_id)])
            elif store_rows[store_id-1][4] == 'Indiana' and store_rows[store_id-1][1] in indiana_city_list:     #If the city is in 'Indiana' and the city is already on the list
                indiana_quantity_list[indiana_city_list.index(store_rows[store_id-1][1])] += order_quantitylist[store_idlist.index(store_id)]

        largest_quantity = 0
        for quantity in indiana_quantity_list:
            if quantity > largest_quantity:
                largest_quantity = quantity

        #prints the answer
        print(f"The city in the state of Indiana with the biggest total sales is '{indiana_city_list[indiana_quantity_list.index(largest_quantity)]}'.")

        # closes the cursors
        first_cursor.close()
        second_cursor.close()

    def query24(self):
        # creates cursors
        first_cursor = self.dbconnection.cursor()
        second_cursor = self.dbconnection.cursor()

        # execute query
        first_cursor.execute("select * from sales.sales_order")
        second_cursor.execute("select * from sales.store_locations")

        # fetch information
        rows = first_cursor.fetchall()
        store_rows = second_cursor.fetchall()

        store_idlist = []
        pricelist = []
        for row in rows:
            if row[10] in store_idlist:     #collect all the prices and put in list
                pricelist[store_idlist.index(row[10])] += row[12]*row[13]
            else:                           #get all store ids and prices and append to lists
                store_idlist.append(row[10])
                pricelist.append(row[12]*row[13])

        city_list = []
        total_price = []
        city_population = []
        for store_id in store_idlist:
            if store_rows[store_id-1][1] in city_list:      #add total prices to each city
                total_price[city_list.index(store_rows[store_id-1][1])] += pricelist[store_idlist.index(store_id)]
            else:                                           #get all cities, prices and populations and add to lists
                city_list.append(store_rows[store_id-1][1])
                total_price.append(pricelist[store_idlist.index(store_id)])
                city_population.append(store_rows[store_id-1][9])

        sales_percapita = []
        for i in range(0, len(total_price)):                #Adds to list the sales per capita of each city
            sales_percapita.append(total_price[i]/city_population[i])

        largest_num = 0
        for num in sales_percapita:
            if num > largest_num:
                largest_num = num

        #prints the answer
        print(f"The city that has the biggest sales per capita is '{city_list[sales_percapita.index(largest_num)]}'.")

        # closes the cursors
        first_cursor.close()
        second_cursor.close()

    def query25(self):
        # creates cursors
        first_cursor = self.dbconnection.cursor()
        second_cursor = self.dbconnection.cursor()

        # execute query
        first_cursor.execute("select * from sales.sales_order")
        second_cursor.execute("select * from sales.products")

        # fetch information
        rows = first_cursor.fetchall()
        product_rows = second_cursor.fetchall()

        product_idlist = []
        product_quantity = []
        for row in rows:
            if row[11] in product_idlist:       #collect all the order quantities and put in list
                product_quantity[product_idlist.index(row[11])] += row[12]
            else:                               #gets all product ids and order quantities and append to lists
                product_idlist.append(row[11])
                product_quantity.append(row[12])

        largest_quantity = 0
        for quantity in product_quantity:
            if quantity > largest_quantity:
                largest_quantity = quantity

        #prints the answer
        print(f"The product with the most sales in units is '{product_rows[product_idlist[product_quantity.index(largest_quantity)]-1][1]}'.")

        # closes the cursors
        first_cursor.close()
        second_cursor.close()

    def query26(self):
        # creates cursors
        first_cursor = self.dbconnection.cursor()
        second_cursor = self.dbconnection.cursor()
        third_cursor = self.dbconnection.cursor()

        # execute query
        first_cursor.execute("select * from sales.sales_order")
        second_cursor.execute("select * from sales.store_locations")
        third_cursor.execute("select * from sales.products")

        # fetch information
        rows = first_cursor.fetchall()
        store_rows = second_cursor.fetchall()
        product_rows = third_cursor.fetchall()

        largest_waterarea = 0
        store_id = 0
        for row in store_rows:      #gets the id of the city with the largest water area
            if row[13] > largest_waterarea:
                largest_waterarea = row[13]
                store_id = row[0]

        product_idlist = []
        product_quantity = []
        for rw in rows:
            if rw[10] == store_id and rw[11] in product_idlist:     #collect all the order quantities and put in list
                product_quantity[product_idlist.index(rw[11])] += rw[12]
            elif rw[10] == store_id:                                #gets all product ids and order quantities and append to lists
                product_idlist.append(rw[11])
                product_quantity.append(rw[12])

        largest_quantity = 0
        for quantity in product_quantity:
            if quantity > largest_quantity:
                largest_quantity = quantity

        #prints the answer
        print(f"The product that has sold more units in the city that has the largest amount of water area is '{product_rows[product_idlist[product_quantity.index(largest_quantity)]-1][1]}'.")

        # closes the cursors
        first_cursor.close()
        second_cursor.close()
        third_cursor.close()

    def query27(self):
        # creates cursors
        first_cursor = self.dbconnection.cursor()
        second_cursor = self.dbconnection.cursor()
        third_cursor = self.dbconnection.cursor()

        # execute query
        first_cursor.execute("select * from sales.regions")
        second_cursor.execute("select * from sales.store_locations")
        third_cursor.execute("select * from sales.sales_order")

        # fetch information
        rows = first_cursor.fetchall()
        st_id_rows = second_cursor.fetchall()
        sales_rows =  third_cursor.fetchall()

        state_list = []
        for row in rows:                    #collects all northeastern states
            if row[2] == 'Northeast':
                state_list.append(row[0])

        store_idlist = []
        for store in st_id_rows:            #collects all store ids that are from northeastern states
            if store[3] in state_list:
                store_idlist.append(store[0])

        delivery_times=[]
        for sale in sales_rows:             #collects all delivery times
            if sale[10] in store_idlist:
                d0 = sale[6] - sale[4]
                delivery_times.append(d0.days)

        average_delivery_time = sum(delivery_times)/len(delivery_times)

        #prints the answer
        print(f"The average delivery time for stores in the northeast region is {average_delivery_time} days.")
        #The question did not specify if it was the average time for stores from the northeast region or sales teams,
        #so i assumed it was stores

        # closes the cursors
        first_cursor.close()
        second_cursor.close()
        third_cursor.close()

    def query28(self):
        # creates cursors
        first_cursor = self.dbconnection.cursor()
        second_cursor = self.dbconnection.cursor()

        # execute query
        first_cursor.execute("select * from sales.sales_order")
        second_cursor.execute("select * from sales.sales_team")

        # fetch information
        rows = first_cursor.fetchall()
        team_rows = second_cursor.fetchall()

        seller_ids = []
        seller_profits = []
        for row in rows:
            if row[8] in seller_ids:        #collect all profits and put in list
                seller_profits[seller_ids.index(row[8])] += row[12]*(row[13]-row[14])
            else:                           #gets all seller ids and profits and append to lists
                seller_ids.append(row[8])
                seller_profits.append(row[12]*(row[13]-row[14]))

        biggest_profit = 0
        for profit in seller_profits:
            if profit > biggest_profit:
                biggest_profit = profit

        #prints the answer
        print(f"The seller with the biggest profit is {team_rows[seller_ids[seller_profits.index(biggest_profit)]-1][1]}.")

        # closes the cursors
        first_cursor.close()
        second_cursor.close()


