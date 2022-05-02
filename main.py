# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import pymongo
import fasta_parser

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    client = pymongo.MongoClient(
        "mongodb+srv://evo:evolutional@evolutional.aweop.mongodb.net/genes?retryWrites=true&w=majority")
    db = client['genes']
    collection = db['sequence']
    print(collection.find_one())
    print_hi('PyCharm')




# See PyCharm help at https://www.jetbrains.com/help/pycharm/
