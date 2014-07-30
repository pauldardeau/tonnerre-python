# Copyright Paul Dardeau, SwampBits LLC 2014
# BSD License

from chaudiere import KeyValuePairs
from tonnerre import Message
from tonnerre import MessageType
from tonnerre import Messaging

#******************************************************************************
#******************************************************************************

if __name__ == '__main__':
   # initialize the Messaging system using our configuration file
   Messaging.initialize('/Users/paul/github/tonnerre/test/tonnerre.ini')
   
   # set up some data for us to send
   kvp = KeyValuePairs()
   kvp.addPair('stooge1', 'Moe')
   kvp.addPair('stooge2', 'Larry')
   kvp.addPair('stooge3', 'Curly')

   msg = Message('echo', MessageType.KeyValues)
   # populate our data
   msg.setKeyValuesPayload(kvp)
   
   # send it
   msgResponse = Message()
   if msg.send('echo_service', msgResponse):
      print "message successfully sent and response received"
      
      # retrieve the response payload
      kvpResponse = msgResponse.getKeyValuesPayload()
      if kvpResponse is not None:
         # print out the keys and values
         kvpResponse.printKeyValues()
      else:
         print "error: no response payload"
   else:
      print "error: unable to send or receive message"


#******************************************************************************
#******************************************************************************

