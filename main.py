from pymongo import MongoClient
import glob
import csv
from multiprocessing import Pool
from multiprocessing import Process
import time
import pandas

class InputClusterer:

	def __init__(self):
		pass

	def add_addresses_to_db(self, file):
		client = MongoClient('localhost', 27017)
		db = client.pymongo_inputClustering
		locked_to = db.locked_to

		for chunk in pandas.read_csv(file, iterator=True, chunksize=500):

			pending_inserts = []
			for row in enumerate(chunk.values):
				row_data = row[1]

				output_id = row_data[0]
				address = row_data[1]

				mongo_data = {"address" : address, "output": output_id}
				pending_inserts.append(mongo_data)

			locked_to.insert_many(pending_inserts)
			print('bulk insert')

	def group_addresses(self, input_file):

		with open(input_file, 'r') as fp:
			csv_reader = csv.reader(fp)
			mongo_locked_to_data = db.locked_to
			collection_tx_to_address = db.tx_to_address
			collection_address_to_txs = db.address_to_txs

			for relation in csv_reader:
				output_id = relation[0]
				txid = relation[1]

				address_document = mongo_locked_to_data.find_one({'output': output_id})

				if address_document is None:
					# print('address is none, skipping....')
					continue 

				address = address_document['address']

				addresses_which_input_a_tx = collection_tx_to_address.find_one({'txid': txid})

				if addresses_which_input_a_tx is None:
					# create a new txid->[address] mapping
					collection_tx_to_address.insert_one({'txid': txid, 'addresses': [address]})
				else:
					# augment the existing txid->[addresses] + [address] mapping
					updated_fields = { "$addToSet": { "addresses": address } }
					collection_tx_to_address.update_one({"_id": addresses_which_input_a_tx["_id"]}, updated_fields)

				address_to_txids = collection_address_to_txs.find_one({'address': address})

				if address_to_txids is None:
					# create a new address -> [txid] mapping
					collection_address_to_txs.insert_one({'address': address, 'txids': [txid]})
				else:
					# update the existing address -> [txids] + [txid] mapping
					updated_fields = { "$addToSet": { "txids": txid } }
					collection_address_to_txs.update_one({"_id": address_to_txids["_id"]}, updated_fields)

	def generate_linked_address_collection(self, address_file):

		with open(address_file, 'r') as fp:
			csv_reader = csv.reader(fp)
			mongo_locked_to_data = db.locked_to
			collection_tx_to_address = db.tx_to_address
			collection_address_to_txs = db.address_to_txs
			address_mappings = db.linked_addresses

			for address_entry in csv_reader:
				address = address_entry[0]
				first_insert = True
				txs_that_address_inputs_doc = collection_address_to_txs.find_one({'address': address})
				if txs_that_address_inputs_doc is None:
					continue

				txs_that_address_inputs = txs_that_address_inputs_doc['txids']

				for txid in txs_that_address_inputs:

					addresses_to_link_doc = collection_tx_to_address.find_one({'txid': txid})
					if addresses_to_link_doc is None:
						continue

					addresses_to_link = set(addresses_to_link_doc['addresses'])

					if first_insert:
						if address in addresses_to_link:
							addresses_to_link.remove(address)

						if len(addresses_to_link) > 0:
							address_mappings.insert_one({'address': address, 'linked_addresses': list(addresses_to_link)})
							first_insert = False

					else:
						updated_addresses = { "$addToSet": { "linked_addresses": list(addresses_to_link) } }
						address_mappings.update_one({'address': address}, updated_addresses)

	def generate_linked_address_csv(self):

		with open('./clustering-relations.csv', 'w') as fp:
			csv_writer = csv.writer(fp)

			for document in db.linked_addresses.find():
				source_address = document['address']
				addresses_to_link = document['linked_addresses']
				for address_to_link in addresses_to_link:

					csv_writer.writerow([source_address, address_to_link, 'INPUT_LINKED'])


client = MongoClient('localhost', 27017)
db = client.pymongo_inputClustering
relation_path = input("Enter relation directory path....")
regex = relation_path + "relations/bitcoin-csv-block-*/relation-locked-to-*.csv"
files = glob.glob(regex)
process_pool = Pool(8)

print('******************* found files matching regex to be {} **************************'.format(files))

clusterer = InputClusterer();

def add_addresses_output_mappings():
	start = time.time()

	procs = [];
	for file in files:
		p = Process(target=clusterer.add_addresses_to_db, args=(file,))
		procs.append(p)
		p.start()

	for proc in procs:
		proc.join()

	end = time.time()
	print('*********** populated mongo db with address->output mappings *****************')
	print('time elapsed: {}'.format(end - start))

def add_grouped_address_data():

	input_regex = relation_path + "relations/bitcoin-csv-block-*/relation-inputs-*.csv"
	input_files = glob.glob(input_regex)

	for input_file in input_files:
		clusterer.group_addresses(input_file)

	print('*********** populated mongo db with clustered address data *****************')

def add_linked_address_result():
	address_file_regex = relation_path + "data/sample-address-data-unique.csv"
	clusterer.generate_linked_address_collection(address_file_regex)

	print('*********** populated mongo db with linked address data *****************')

def generate_csv():

	clusterer.generate_linked_address_csv()
	print('************** csv generation complete *************************')

