import xlsxwriter
from queries import get_haplogroups, each_with_each, get_wild_type, wild_type_to_base_poly, calc_wild_type, calculate_formulas,distances_with_base, population_to_base_poly
import pymongo
import os
import time


tasks = [{"regions": [], "databases": ["Ukraine"], "name": "UKR", "number": 1},
         {"regions": ["ZA", "ST", "IF"], "databases": [], "name": "UKR_Karpatska", "number": 2},
         {"regions": ["KHM", "RO","CH","KHA","SU","ZH","BG"], "databases": [], "name": "UKR_Tsenralno_ukr", "number": 3}
         ]

def print_formulas(worksheet, col, row, calculations):
  worksheet.write(row, col, "Мат. сподівання")
  worksheet.write(row, col + 1, "Серєднє квадратичне відхилення")
  worksheet.write(row, col + 2, "Мода")
  worksheet.write(row, col + 3, "Мінімум")
  worksheet.write(row, col + 4, "Максимум")
  worksheet.write(row, col + 5, "Коеф. варіації")
  worksheet.write(row + 1, col, calculations['mat_spod'])
  worksheet.write(row + 1, col + 1, calculations['std'])
  worksheet.write(row + 1, col + 2, calculations['mode'])
  worksheet.write(row + 1, col + 3, calculations['min'])
  worksheet.write(row + 1, col + 4, calculations['max'])
  worksheet.write(row + 1, col + 5, calculations['variationCoef'])


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
    start = time.time()
    tasks_collection.insert_one({
      '_id': task['number'],
      "name": task['name']
    })
    haplogroups = get_haplogroups(db, task)
    calc_wild_type(db, task)
    distances_each_with_each = each_with_each(db, task)
    distances_with_base_rCRS = distances_with_base(db, task, "ANDREWS")
    distances_with_base_rSRS = distances_with_base(db, task, "EVA")
    distances_with_wild = distances_with_base(db, task)
    calculations_ewe = calculate_formulas(db, task, 'ewe')
    calculations_rCRS = calculate_formulas(db, task, "ANDREWS")
    calculations_rSRS = calculate_formulas(db, task, "EVA")
    calculations_wild = calculate_formulas(db, task, "EVA")

    wild_type = get_wild_type(db, task)
    rCRS_poly = wild_type_to_base_poly(db, "ANDREWS", task)
    rSRS_poly = wild_type_to_base_poly(db, "EVA", task)
    rCRS_poly_population = population_to_base_poly(db, "ANDREWS", task)
    rSRS_poly_population = population_to_base_poly(db, "EVA", task)
    end = time.time()
    total_time = end - start

    print("Task-" + str(task['number']) + " time: " + str(total_time))

    row = 0
    col = 0
    worksheet = workbook.add_worksheet(task['name'] + "-" + str(task['number']))

    worksheet.write(row, col, "Відстань")
    worksheet.write(row + 1, col, "Розподіл відносно базової rCRS")
    worksheet.write(row + 2, col, "Розподіл відносно базової rCRS (частка)")
    col += 1

    for distance in distances_with_base_rCRS:
      worksheet.write(row, col, distance['_id'])
      worksheet.write(row + 1, col, distance['count'])
      worksheet.write(row + 2, col, distance['percent'])
      col += 1
    col = 1
    row += 3
    print_formulas(worksheet, col, row, calculations_rCRS)

    row += 3
    col = 0
    worksheet.write(row, col, "Відстань")
    worksheet.write(row + 1, col, "Розподіл відносно базової rSRS")
    worksheet.write(row + 2, col, "Розподіл відносно базової rSRS (частка)")
    col += 1
    for distance in distances_with_base_rSRS:
      worksheet.write(row, col, distance['_id'])
      worksheet.write(row + 1, col, distance['count'])
      worksheet.write(row + 2, col, distance['percent'])
      col += 1
    col = 1
    row += 3
    print_formulas(worksheet, col, row, calculations_rSRS)

    row += 3
    col = 0

    worksheet.write(row, col, "Відстань")
    worksheet.write(row + 1, col, "Розподіл відносно дикого типу")
    worksheet.write(row + 2, col, "Розподіл відносно дикого типу (частка)")
    col += 1

    for distance in distances_with_wild:
      worksheet.write(row, col, distance['_id'])
      worksheet.write(row + 1, col, distance['count'])
      worksheet.write(row + 2, col, distance['percent'])
      col += 1
    col = 1
    row += 3
    print_formulas(worksheet, col, row, calculations_wild)

    row += 3
    col = 0

    worksheet.write(row, col, "Відстань")
    worksheet.write(row + 1, col, "Розподіл відносно попарних")
    worksheet.write(row + 2, col, "Розподіл відносно попарних (частка)")
    col += 1

    for distance in distances_each_with_each:
      worksheet.write(row, col, distance['_id'])
      worksheet.write(row + 1, col, distance['count'])
      worksheet.write(row + 2, col, distance['percent'])
      col += 1
    col = 1
    row += 3
    print_formulas(worksheet, col, row, calculations_ewe)


    row += 3
    col = 0
    worksheet.write(row, col, "Дикий тип")
    worksheet.write(row, col + 1, wild_type)


    # print("POLY!", rCRS_poly)
    row += 1
    col = 0
    worksheet.write(row, col, "Кількість поліморфізмів у дикого типу відносно базової rCRS")
    worksheet.write(row, col + 1, rCRS_poly[0]["_id"])


    row += 1
    col = 0
    worksheet.write(row, col, "Кількість поліморфізмів у дикого типу відносно базової rSRS")
    worksheet.write(row, col + 1, rSRS_poly[0]["_id"])


    # print("POLY! Population", rCRS_poly)
    row += 1
    col = 0
    worksheet.write(row, col, "Кількість поліморфізмів у популяції відносно базової rCRS")
    worksheet.write(row, col + 1, rCRS_poly_population[0]["count"])


    row += 1
    col = 0
    worksheet.write(row, col, "Кількість поліморфізмів у популяції відносно базової RSRS")
    worksheet.write(row, col + 1, rSRS_poly_population[0]["count"])

    row += 3
    col = 0
    worksheet.write(row, col, "Гаплогрупа")
    worksheet.write(row + 1, col, "Кількість представників")
    col += 1
    for haplo in haplogroups:
      worksheet.write(row, col, haplo['_id'])
      worksheet.write(row + 1, col, haplo['count'])
      col += 1



  workbook.close()


if __name__ == '__main__':
  create_excel()
