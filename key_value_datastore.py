import os
import uuid
import json
import mmap
import sys
import threading
import time
from collections import OrderedDict


MAX_KEY_LEN = 32  # characters
MAX_VALUE_SIZE = 16 * 1024  # 16 Kbytes
MAX_LOCAL_STORAGE_SIZE = 1 * 1024 * 1024 * 1024  # 1 GB
PATH = "./data"  # folder where file will be created


def check(value = None, value_type="key"):  

    """
        Method to check whether a key is string and value is JSON object
    """
    if value_type == "key":
        if not isinstance(value, str) or value == "" or value is  None:
            raise ValueError(f"Key '{value}' must be of type str and length greater than 0 .")
            return False
        return len(value) <= MAX_KEY_LEN
    elif value_type == "value":
        if not isinstance(value, dict) or value == {} or value is  None :        
            raise ValueError(f"value '{value}' must be of type dict and length greater than 0 .")
            return False
        return sys.getsizeof(value) <= MAX_VALUE_SIZE


class InitialiseObject: 
    """
       Class to initiate object with key, value and time to live
    """

    def __init__(self, value, created_at, time_to_live):
        self.value = value
        self.time_to_live = time_to_live
        self.created_at = created_at

    def is_expired(self):
        if self.time_to_live is None:
            return False
        current_time = int(time.time() * 1000)
        return (current_time - self.created_at) > self.time_to_live * 1000



class DataStore:
    def __init__(self, file, file_name):        
        self.file = file
        self.file_name = file_name
        self.mmaped_val = self.change_file_size()
        self.data = OrderedDict()
        self.lock = threading.Lock()
        self.convert_to_json()

    def change_file_size(self): 
        """

        If the size of file is greater than 1GB it resizes it to 1GB, converts file data to bytes

        """
        try:
            mmaped_file = mmap.mmap(self.file, 0, access=mmap.ACCESS_WRITE)
            if sys.getsizeof(mmaped_file) > MAX_LOCAL_STORAGE_SIZE:
                mmaped_file.resize(MAX_LOCAL_STORAGE_SIZE)
            return mmaped_file
        except mmap.error:
            raise Exception("Error : Length of file is zero")


    def convert_to_json(self) : 
        """
           Converts data from bytes to string and creates a JSON object
        """
        
        raw_data = self.mmaped_val[:].decode('ascii').rstrip('\0')
        self.data = json.loads(raw_data)


    def create_key(self, key, value, time_to_live=None) : 
        """
            Creates a new dictionary for the given key with the value in datastore if ,
                1. The key is not already present in datastore. 
                2. Both key and value are less than the given size
                3. If time to live is provided, it must be an integer in seconds
        """

        with self.lock:
            if key in self.data:
                raise ValueError(f"Key '{key}' is already present in datastore.")

            if check(key, value_type="key") and check(value, value_type="value"):
                if time_to_live is not None:
                    try:
                        time_to_live = int(time_to_live)
                    except:
                        raise ValueError(f"Time to live {time_to_live} must be an integer value.")

                parameters = [value, int(time.time() * 1000), time_to_live]
                self.data[key] = parameters

                with open(self.file_name, 'ab') as f:
                    print("file opened, writing ......")
                    string = f"'{key}'" + " : " + str(parameters[0]) + "\n"
                    f.write(bytes(string.encode('ascii')))
                    print("successfully written on file")
                
            else:
                raise ValueError(f"Key or value is empty or exceeds allowed size, allowed size for key, '{MAX_KEY_LEN}', for value , '{MAX_VALUE_SIZE}'")


    def delete_key(self, key): 
        """
            Deletes the key-value pair from data.
            If key is not present it gives a message.

        """

        with self.lock:
            if key not in self.data:
                raise ValueError(f"'{key}' , not present in datastore")
                return  # return if key is non-existent

            print(f"Deleting key, '{key}' ")
            del self.data[key]


    def get_key(self, key):     
        """
            Get the value in data for the given key.
            If key is not present, gives a message that 'Key not present in datastore'
           Checks whether a key's time to live has expired or not, if expired gives message that "Time to live expired"
        
        """
        with self.lock:

            if key not in self.data:
                raise ValueError(f"Key '{key}' not present in datastore.")
            value = InitialiseObject(*self.data.get(key)) 

            if value.is_expired():
                del self.data[key]
                raise ValueError(f"Key '{key}' Time to live has expired.")

            return value.value


def create_file_name():        
    """
    Creates a file name , if not provided by user
    """
    
    file_name = uuid.uuid4().int
    return "LOCAL_STORAGE_{}".format(file_name)



def create_object(file_name=None) :      
    """
    Create a new datastore of name file_name
    """
    # file_name = file_name.lstrip() + file_name.rstrip()
    if file_name is None or file_name.isspace():
        file_name = create_file_name()
    file_name = f"{PATH}/{file_name}"
    file = os.open(file_name, os.O_CREAT | os.O_RDWR)

    """
        App lock on file, so that only single user can access it at once.
    """
    print(f"Acquiring lock on file {file_name}")
    print(f"Lock acquired on file {file_name}")

    if not os.path.isfile(file_name) or os.fstat(file).st_size == 0:
        with open(file_name, 'ab') as f:
            string = "{}\n"
            f.write(bytes(string.encode('ascii')))
    else:
        raise ValueError("File name must not be empty")
    return DataStore(file,file_name)
