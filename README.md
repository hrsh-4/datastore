<h1>File based Key-Value datastore</h1>

<h3>Supports basic CRD (Create, Read, Delete)</h3>

<strong>



Functionalities:



</strong>




<ul>
<li>It can be initialized using an optional file path. If not provided, it will reliably create itself in a reasonable location on the laptop </li>
<li>A new key-value pair can be added to the data store using the Create operation. The key is always a string - capped at 32chars. The value is always a JSON object - capped at 16KB. </li>
  
 <li>A Read operation on a key can be performed by providing the key, and receiving the value in response, as a JSON object.</li>
 <li>A Delete operation can be performed by providing the key</li>
  
<li>Every key supports setting a Time To Live property when it is created. This property is optional. If provided, it will be evaluated as an integer defining the number of seconds the key must be retained in the data store. Once the Time T -Live for a key has expired, the key will no longer be available for Read or Delete operations.</li>

<li>The size of the file storing data  never exceeds 1GB</li>

<li>More than one client process will not be allowed to use the same file as a data store at any given time and the datastore is thread safe.</li>

</ul>

<h2> Example </h2>

<pre>
from key_value_datastore import *

object = create_object("new_file")   # new file with name "new_file" will be created
</pre>

If file name is not provided in <code> create_object() </code>, it will generate 128-bit long random file name.

<pre>
object.create_key("test_key",{"value" : 1000},300) # "test_key" will be created in the file and will be accessible till 300 seconds

object.get_key("test_key")                         # returns value of "test_key", if key found and  time to live not expired 

object.delete_key("test_key")                      # deletes key "test_key", if key found
</pre>
