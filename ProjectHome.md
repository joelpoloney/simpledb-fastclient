Implements a parallelized version of the Amazon SimpleDB client. This client utilizes cURL and the curl\_multi library to send the requests.

This class requires changes to the original Amazon\_SimpleDB\_Client class. All of the private classes will need to be made protected. This is so that the FastClient can inherit them from the original client.

Currently getAttributes, putAttributes, and deleteAttributes have been implemented. Support for the other interface functions will be added in the future.

**Note:** With the latest version of Amazon's PHP SimpleDB API, they have changed the ServiceURL from http://sdb.amazonaws.com/ to https://sdb.amazonaws.com/. Currently, secure http doesn't work with this client and I am working on a solution around it. For the time being, change the ServiceURL to point back at http://sdb.amazonaws.com/.