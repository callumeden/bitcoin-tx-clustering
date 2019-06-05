from pymongo import MongoClient
import glob
import csv

class InputClusterer:

	def __init__(self):
		pass

	def add_addresses_to_db(self, file):
		with open(file, 'r') as fp:
			csv_reader = csv.reader(fp)
			locked_to = db.locked_to

			batched_inserts = [];
			for relation in csv_reader:

				output_id = relation[0]
				address = relation[1]

				mongo_data = {"address" : address, "output": output_id}
				batched_inserts.append(mongo_data)

			print("performing batched mongo db insert....")
			locked_to.insert_many(batched_inserts)
			print("....completed batched mongo db insert")

	def cluster(self, input_file):

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
					print('address is none, skipping....')
					continue 

				address = address_document['address']

				addresses_which_input_a_tx = collection_tx_to_address.find_one({'txid': txid})

				if addresses_which_input_a_tx is None:
					# create a new txid->[address] mapping
					collection_tx_to_address.insert_one({'txid': txid, 'addresses': [address]})
					print('written new data txid {} -> address'.format(txid))
				else:
					# augment the existing txid->[addresses] + [address] mapping
					current_addresses = addresses_which_input_a_tx['addresses']
					current_addresses.append(address)
					updated_fields = { "$set": { "addresses": current_addresses } }
					collection_tx_to_address.update_one({"_id": addresses_which_input_a_tx["_id"]}, updated_fields)

				address_to_txids = collection_address_to_txs.find_one({'address': address})

				if address_to_txids is None:
					# create a new address -> [txid] mapping
					collection_address_to_txs.insert_one({'address': address, 'txids': [txid]})
					print('written new data address {} -> txids'.format(address))
				else:
					# update the existing address -> [txids] + [txid] mapping
					current_txids = address_to_txids['txids']
					print('current txids {}'.format(current_txids))

					current_txids.append(txid)
					print('new txids {}'.format(current_txids))

					updated_fields = { "$set": { "txids": current_txids } }
					collection_address_to_txs.update_one({"_id": address_to_txids["_id"]}, updated_fields)


client = MongoClient('localhost', 27017)
db = client.pymongo_inputClustering
relation_path = input("Enter relation directory path....")
regex = relation_path + "bitcoin-csv-block-*/relation-locked-to-*.csv"
files = glob.glob(regex)

print('******************* found files matching regex to be {} **************************'.format(files))

clusterer = InputClusterer();

for file in files:
	clusterer.add_addresses_to_db(file)

print('*********** populated mongo db with address-> output mappings *****************')


input_regex = relation_path + "bitcoin-csv-block-*/relation-inputs-*.csv"
input_files = glob.glob(input_regex)

for input_file in input_files:
	clusterer.cluster(input_file)


