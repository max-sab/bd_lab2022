import xlsxwriter
from queries import get_haplogroups, each_with_each, get_wild_type, wild_type_to_base_poly
import pymongo
import os


tasks = [{"regions": [], "databases": ["Ukraine"], "name": "UKR"}, {"regions": ["ZA", "ST", "IF"], "databases": [], "name": "UKR_Karpatska"}]


def create_excel():
  if os.path.exists('Result.xlsx'):
    os.remove('Result.xlsx')
  workbook = xlsxwriter.Workbook('Result.xlsx')
  client = pymongo.MongoClient(
    "mongodb+srv://evo:evolutional@evolutional.aweop.mongodb.net/genes?retryWrites=true&w=majority")
  db = client['genes']
  for index, task in enumerate(tasks):
    row = 0
    col = 0
    worksheet = workbook.add_worksheet(task['name'] + "-" + str(index))
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
    row += 1
    worksheet.write(row, col, "Розподіл відносно попарних")
    worksheet.write(row + 1, col, "Розподіл відносно попарних (частка)")
    col += 1
    distances_each_with_each = each_with_each(db, task['regions'], task['databases'])
    print(distances_each_with_each)
    for distance in distances_each_with_each:
      print(distance)
      worksheet.write(row, col, distance['_id'])
      worksheet.write(row + 1, col, distance['count'])
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
    worksheet.write(row, col + 1, rSRS_poly[0]["_id"] - 1)


  workbook.close()


if __name__ == '__main__':
  create_excel()
