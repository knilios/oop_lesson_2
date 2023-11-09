import csv, os, math
import numpy as np

def load_data_from_database(file_name):
    data = []
    __location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
    with open(os.path.join(__location__, file_name)) as f:
        rows = csv.DictReader(f)
        for r in rows:
            data.append(dict(r))
        return data
    

cities = load_data_from_database('Cities.csv')
countries = load_data_from_database('Countries.csv')
players = load_data_from_database('Players.csv')
teams = load_data_from_database('Teams.csv')


class DB:
    def __init__(self):
        self.database = []

    def insert(self, table):
        self.database.append(table)

    def search(self, table_name):
        for table in self.database:
            if table.table_name == table_name:
                return table
        return None
    
import copy
class Table:
    def __init__(self, table_name, table):
        """
        :param: table can be either a list of data or data file's name
        """
        self.table_name = table_name
        if type(table) == str:
            self.table = load_data_from_database(table)
        else:
            self.table = table
    
    def join(self, other_table, common_key):
        joined_table = Table(self.table_name + '_joins_' + other_table.table_name, [])
        for item1 in self.table:
            for item2 in other_table.table:
                if item1[common_key] == item2[common_key]:
                    dict1 = copy.deepcopy(item1)
                    dict2 = copy.deepcopy(item2)
                    dict1.update(dict2)
                    joined_table.table.append(dict1)
        return joined_table
    
    def filter(self, condition):
        filtered_table = Table(self.table_name + '_filtered', [])
        for item1 in self.table:
            if condition(item1):
                filtered_table.table.append(item1)
        return filtered_table
    
    def aggregate(self, function, aggregation_key):
        temps = []
        for item1 in self.table:
            temps.append(float(item1[aggregation_key]))
        return function(temps)
    
    def select(self, attributes_list):
        temps = []
        for item1 in self.table:
            dict_temp = {}
            for key in item1:
                if key in attributes_list:
                    dict_temp[key] = item1[key]
            temps.append(dict_temp)
        return temps

    def __str__(self):
        return self.table_name + ':' + str(self.table)

table1 = Table('cities', cities)
table2 = Table('countries', countries)
table3 = Table('teams', "Teams.csv")
table4 = Table('players', "Players.csv")
table5 = Table('titanic', "Titanic.csv")
my_DB = DB()
my_DB.insert(table1)
my_DB.insert(table2)
my_DB.insert(table3)
my_DB.insert(table4)
my_DB.insert(table5)
my_table1 = my_DB.search('cities')

print("Test filter: only filtering out cities in Italy") 
my_table1_filtered = my_table1.filter(lambda x: x['country'] == 'Italy')
print(my_table1_filtered)
print()

print("Test select: only displaying two fields, city and latitude, for cities in Italy")
my_table1_selected = my_table1_filtered.select(['city', 'latitude'])
print(my_table1_selected)
print()

print("Calculting the average temperature without using aggregate for cities in Italy")
temps = []
for item in my_table1_filtered.table:
    temps.append(float(item['temperature']))
print(sum(temps)/len(temps))
print()

print("Calculting the average temperature using aggregate for cities in Italy")
print(my_table1_filtered.aggregate(lambda x: sum(x)/len(x), 'temperature'))
print()

print("Test join: finding cities in non-EU countries whose temperatures are below 5.0")
my_table2 = my_DB.search('countries')
my_table3 = my_table1.join(my_table2, 'country')
my_table3_filtered = my_table3.filter(lambda x: x['EU'] == 'no').filter(lambda x: float(x['temperature']) < 5.0)
print(my_table3_filtered.table)
print()
print("Selecting just three fields, city, country, and temperature")
print(my_table3_filtered.select(['city', 'country', 'temperature']))
print()

print("Print the min and max temperatures for cities in EU that do not have coastlines")
my_table3_filtered = my_table3.filter(lambda x: x['EU'] == 'yes').filter(lambda x: x['coastline'] == 'no')
print("Min temp:", my_table3_filtered.aggregate(lambda x: min(x), 'temperature'))
print("Max temp:", my_table3_filtered.aggregate(lambda x: max(x), 'temperature'))
print()

print("Print the min and max latitude for cities in every country")
for item in my_table2.table:
    my_table1_filtered = my_table1.filter(lambda x: x['country'] == item['country'])
    if len(my_table1_filtered.table) >= 1:
        print(item['country'], my_table1_filtered.aggregate(lambda x: min(x), 'latitude'), my_table1_filtered.aggregate(lambda x: max(x), 'latitude'))
print()

print("What player on a team with “ia” in the team name played less than 200 minutes and made more than 100 passes? Here's the answer!: ")
my_table6 = my_DB.search("players")
my_table6_filtered = my_table6.filter(lambda x: "ia" in x['team']).filter(lambda x: int(x['minutes']) < 200).filter(lambda x: int(x['passes']) > 100)
for item in my_table6_filtered.table:
    print(item['surname'], "from", item['team'])
print()

print("The average number of games played for teams ranking below 10 versus teams ranking above or equal 10. Here they are!: ")
my_table7 = my_DB.search("teams")
above_ten = my_table7.filter(lambda x: int(x['ranking']) <= 10)
below_ten = my_table7.filter(lambda x: int(x['ranking']) > 10)
average_above = np.average(list(map(lambda x: int(x['games']), above_ten.select(["games"]))))
average_below = np.average(list(map(lambda x: int(x['games']), below_ten.select(["games"]))))
print(f"Above: {average_above:.2f} vs. Below: {average_below:.2f}")
print()

print("The average number of passes made by forwards versus by midfielders")
my_table8 = my_DB.search("players")
fowards_passes = my_table8.filter(lambda x: x["position"] == "forward")
midfielders_passes = my_table8.filter(lambda x: x['position'] == 'midfielder')
avg_fow = np.average(list(map(lambda x: int(x['passes']), fowards_passes.select(["passes"]))))
avg_mid = np.average(list(map(lambda x: int(x['passes']), midfielders_passes.select(["passes"]))))
print(f"fowards: {avg_fow:.2f} vs. midfielders: {avg_mid:.2f}")
print()

print('The average fare paid by passengers in the first class versus in the third class')
titanic_table = my_DB.search("titanic")
first_class = titanic_table.filter(lambda x: x['class'] == '1')
third_class = titanic_table.filter(lambda x: x['class'] == '3')
avg_f = np.average(list(map(lambda x: float(x['fare']), first_class.select(["fare"]))))
avg_t = np.average(list(map(lambda x: float(x['fare']), third_class.select(["fare"]))))
print(f"first class: {avg_f:.2f} vs. third class: {avg_t:.2f}")
print()

print("The survival rate of male versus female passengers: ")
titanic_table = my_DB.search("titanic")
male = titanic_table.filter(lambda x: x['gender'] == 'M').select(["gender", "survived"])
female = titanic_table.filter(lambda x: x['gender'] == 'F').select(["gender", "survived"])
survival_m = sum([1 if i["survived"] == 'yes' else 0 for i in male]) / len(male)
survival_f = sum([1 if i["survived"] == 'yes' else 0 for i in female]) / len(female)
print(f"male: {survival_m*100:.2f}% vs. female: {survival_f*100:.2f}%")
print()
