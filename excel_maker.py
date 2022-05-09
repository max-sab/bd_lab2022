import xlsxwriter
from queries import get_haplogroups, each_with_each, get_wild_type, wild_type_to_base_poly, find_percentage, calculate_formulas, population_to_base_poly
import pymongo
import os
import time


tasks = [{"regions": [], "databases": ["Ukraine"], "name": "UKR", "number": 1},
         {"regions": ["ZA", "ST", "IF"], "databases": [], "name": "UKR_Karpatska", "number": 2},
         {"regions": ["KHM", "RO","CH","KHA","SU","ZH","BG"], "databases": [], "name": "UKR_Tsenralno_ukr", "number": 3}
         ]


def create_excel():
  if os.path.exists('Result.xlsx'):
    os.remove('Result.xlsx')
  workbook = xlsxwriter.Workbook('Result.xlsx')
  client = pymongo.MongoClient(
    "mongodb+srv://evo:evolutional@evolutional.aweop.mongodb.net/genes?retryWrites=true&w=majority")
  db = client['genes']
  db['tasks'].delete_many({})
  db['distributions'].delete_many({})
  if 'tasks' not in db.list_collection_names():
    db.create_collection('tasks')
  if 'distributions' not in db.list_collection_names():
    db.create_collection('distributions')
  for task in tasks:
    tasks_collection = db['tasks']
    tasks_collection.insert_one({
      '_id': task['number'],
      "name": task['name']
    })
    start = time.time()
    row = 0
    col = 0
    worksheet = workbook.add_worksheet(task['name'] + "-" + str(task['number']))
    haplogroups = get_haplogroups(db, task['regions'], task['databases'])
    worksheet.write(row, col, "Гаплогрупа")
    worksheet.write(row+1, col, "Кількість представників")
    col += 1
    for haplo in haplogroups:
      worksheet.write(row, col, haplo['_id'])
      worksheet.write(row+1, col, haplo['count'])
      col += 1
    row += 3
    col = 0
    worksheet.write(row, col, "Відстань")
    worksheet.write(row + 1, col, "Розподіл відносно попарних")
    worksheet.write(row + 2, col, "Розподіл відносно попарних (частка)")
    col += 1
    distances_each_with_each = each_with_each(db, task)
    ewe_percentage = find_percentage(db, task, 'ewe')
    calculations = calculate_formulas(db, task, 'ewe')
    for distance in distances_each_with_each:
      worksheet.write(row, col, distance['_id'])
      worksheet.write(row + 1, col, distance['count'])
      col += 1
    col = 1
    for percent in ewe_percentage:
      worksheet.write(row + 2, col, percent['percent'])
      col += 1
    col += 1

    wild_type = get_wild_type(db, task['regions'], task['databases'])
    row += 3
    col = 0
    worksheet.write(row, col, "Дикий тип")
    worksheet.write(row, col + 1, wild_type[0]["letters"])

    rCRS_poly = wild_type_to_base_poly(db, "ANDREWS", task['regions'], task['databases'])
    print("POLY!", rCRS_poly)
    row += 1
    col = 0
    worksheet.write(row, col, "Кількість поліморфізмів у дикого типу відносно базової rCRS")
    worksheet.write(row, col + 1, rCRS_poly[0]["_id"])

    rSRS_poly = wild_type_to_base_poly(db, "EVA", task['regions'], task['databases'])
    row += 1
    col = 0
    worksheet.write(row, col, "Кількість поліморфізмів у дикого типу відносно базової rSRS")
    worksheet.write(row, col + 1, rSRS_poly[0]["_id"])

    rCRS_poly_population = population_to_base_poly(db, "ANDREWS", task['regions'], task['databases'])
    print("POLY! Population", rCRS_poly)
    row += 1
    col = 0
    worksheet.write(row, col, "Кількість поліморфізмів у популяції відносно базової rCRS")
    worksheet.write(row, col + 1, rCRS_poly_population[0]["count"])

    rSRS_poly_population = population_to_base_poly(db, "EVA", task['regions'], task['databases'])
    row += 1
    col = 0
    worksheet.write(row, col, "Кількість поліморфізмів у популяції відносно базової RSRS")
    worksheet.write(row, col + 1, rSRS_poly_population[0]["count"])
    end = time.time()
    total_time = end - start
    print("Task-"+str(task['number']) + " time: " + str(total_time))


  workbook.close()


if __name__ == '__main__':
  create_excel()
