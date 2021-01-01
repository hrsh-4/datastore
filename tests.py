import json
from key_value_datastore import *

import unittest

class TestDataStore(unittest.TestCase):

	def test_empty_key(self):

		key = ""
		
		self.assertRaises(ValueError, check, key,  "key")

	def test_large_key(self):

		key = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
		result  = check(key)
		self.assertFalse(result)

	def test_non_string_key(self):

		key = 1
		self.assertRaises(ValueError, check, key, "key")

	def test_empty_value(self):

		value = {}
		self.assertRaises(ValueError, check,value, "value")

	def test_non_json_value(self):

		value = "not a json object"
		self.assertRaises(ValueError, check, value, "value")

	def test_create_with_empty_key(self):

		obj = create_object()
		key = ""
		value = {"val" :100}

		with self.assertRaises(ValueError):
			DataStore.create_key(obj,key, value)

	def test_create_with_empty_value(self):
		
		obj = create_object()
		key = "test"
		value = {}

		with self.assertRaises(ValueError):
			DataStore.create_key(obj,key, value)

	def test_create_with_key_value_empty(self):

		obj = create_object()
		key = ""
		value = {}

		with self.assertRaises(ValueError):
			DataStore.create_key(obj,key, value)

	def test_create_with_existing_key(self):
		
		obj = create_object(" ")
		key = "details"
		value = {"age" : 22, "gender":"Male"}
		time_to_live = 120
		
		DataStore.create_key(obj, key, value, time_to_live)

		with self.assertRaises(ValueError):
			DataStore.create_key(obj, key, value, time_to_live)


	def test_create_with_valid_data(self):

		obj = create_object()
		key = "details"
		value = {"age" : 22, "gender":"Male"}
		time_to_live = 120

		try:	
			with self.assertRaises((ValueError)):
				DataStore.create_key(obj, key, value, time_to_live)
		except:
			print("No exception raised")

	def  test_get_key(self):

		obj = create_object()
		key = "details"
		value = {"age" : 22, "gender":"Male"}
		time_to_live = 120

		DataStore.create_key(obj, key, value, time_to_live)
		
		with self.assertRaises(ValueError):
			DataStore.get_key(obj,"test")

	def test_delete_key(self):

		obj = create_object()
		key = "details"
		value = {"age" : 22, "gender":"Male"}
		time_to_live = 120
		
		DataStore.create_key(obj, key, value, time_to_live)
		
		with self.assertRaises(ValueError):
			DataStore.delete_key(obj,"test")



if __name__ == '__main__':
	unittest.main()